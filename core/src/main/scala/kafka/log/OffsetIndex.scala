/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import org.apache.kafka.common.utils.Utils

import scala.math._
import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.locks._
import kafka.utils._
import kafka.utils.CoreUtils.inLock
import kafka.common.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * 
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * 
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * 
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an 
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * 
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * 
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the 
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * 
 * The frequency of entries is up to the user of this class.
 * 
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal 
 * storage format.
  * @param _file 指向磁盘上的索引文件
  * @param baseOffset 对应日志文件中第一条消息的offset
  * @param maxIndexSize 当前索引文件中最多能够保存的索引项个数
 */
class OffsetIndex(@volatile private[this] var _file: File, val baseOffset: Long, val maxIndexSize: Int = -1) extends Logging {

  // 在对mmap操作时需要加锁保护
  private val lock = new ReentrantLock
  
  /**
    * initialize the memory mapping for this index
    * 用来操作索引文件的MappedByteBuffer
    * */
  @volatile
  private[this] var mmap: MappedByteBuffer = {
    // 如果索引文件不存在，则创建新文件并返回true，反之返回false
    val newlyCreated = _file.createNewFile()
    // 根据文件创建RandomAccessFile可读写对象
    val raf = new RandomAccessFile(_file, "rw")
    try {
      /**
        * pre-allocate the file if necessary
        * 对于新创建的索引文件，进行扩容
        * */
      if (newlyCreated) {
        if (maxIndexSize < 8)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        // 根据maxIndexSize的值对索引文件进行扩容，扩容结果是小于maxIndexSize的最大的8的倍数
        raf.setLength(roundToExactMultiple(maxIndexSize, 8))
      }

      /**
        * memory-map the file
        * 进行内存映射
        **/
      val len = raf.length()
      val idx = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, len)

      /* set the position in the index for the next entry */
      if (newlyCreated)
        // 将新创建的索引文件的position设置为0，从头开始写文件
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is all valid and set position to last entry
      /**
        * 对于原来就存在的索引文件，则将position移动到所有索引项的结束为止，防止数据覆盖，
        * idx是一个Buffer，limit表示目前数据的截止位置
        */
        idx.position(roundToExactMultiple(idx.limit, 8))
      // 返回MappedByteBuffer
      idx
    } finally {
      CoreUtils.swallow(raf.close())
    }
  }

  /**
    * the number of eight-byte entries currently in the index
    * 当前索引文件中的索引项个数
    * */
  @volatile
  private[this] var _entries = mmap.position / 8

  /**
    * The maximum number of eight-byte entries this index can hold
    * 当前索引文件中最多能够保存的索引项个数
    * */
  @volatile
  private[this] var _maxEntries = mmap.limit / 8

  // 最后一个索引项的offset
  @volatile
  private[this] var _lastOffset = readLastEntry.offset
  
  debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d"
    .format(_file.getAbsolutePath, _maxEntries, maxIndexSize, _entries, _lastOffset, mmap.position))

  /** The maximum number of entries this index can hold */
  def maxEntries: Int = _maxEntries

  /** The last offset in the index */
  def lastOffset: Long = _lastOffset

  /** The index file */
  def file: File = _file

  /**
   * The last entry in the index
   */
  def readLastEntry(): OffsetPosition = {
    inLock(lock) {
      _entries match {
        // 当没有索引项时，返回以baseOffset为offset，0为position的OffsetPosition
        case 0 => OffsetPosition(baseOffset, 0)
        /**
          * 当有索引项时，需要计算offset和position
          * relativeOffset(mmap, s - 1)会获取mmap中第s-1个索引项的索引
          * physical(mmap, s - 1)会获取mmap中第s-1个索引项所映射的物理位置
          */
        case s => OffsetPosition(baseOffset + relativeOffset(mmap, s - 1), physical(mmap, s - 1))
      }
    }
  }

  /**
   * Find the largest offset less than or equal to the given targetOffset 
   * and return a pair holding this offset and its corresponding physical file position.
   * 
   * @param targetOffset The offset to look up.
   * 
   * @return The offset found and the corresponding file position for this offset. 
   * If the target offset is smaller than the least entry in the index (or the index is empty),
   * the pair (baseOffset, 0) is returned.
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    // Windows系统需要加锁
    maybeLock(lock) {
      // 创建一个mmap的副本
      val idx = mmap.duplicate
      // 查找targetOffset在idx中操作
      val slot = indexSlotFor(idx, targetOffset)
      if(slot == -1)
        // 找不到
        OffsetPosition(baseOffset, 0)
      else
        // 将offset和物理地址封装为offsetPosition对象并返回
        OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot))
      }
  }
  
  /**
   * Find the slot in which the largest offset less than or equal to the given
   * target offset is stored.
    * 找到小于或等于给定targetOffset的最大offset的槽位（即索引）
   * 
   * @param idx The index buffer 索引项Buffer
   * @param targetOffset The offset to look for 需要查找的offset
   * 
   * @return The slot found or -1 if the least entry in the index is larger than the target offset or the index is empty
   */
  private def indexSlotFor(idx: ByteBuffer, targetOffset: Long): Int = {
    // we only store the difference from the base offset so calculate that
    // 根据baseOffset计算相对offset
    val relOffset = targetOffset - baseOffset
    
    // check if the index is empty
    // 如果文件中没有索引项，直接返回-1
    if (_entries == 0)
      return -1
    
    // check if the target offset is smaller than the least offset
    /**
      * 找到第0个offset的相对baseOffset的偏移量
      * 如果第0个索引的相对偏移量都大于给定的targetOffset的相对偏移量
      * 说明没有符合条件的offset，直接返回-1
      */
    if (relativeOffset(idx, 0) > relOffset)
      return -1
      
    // binary search for the entry
    // 进行二分搜索查找符合的offset
    var lo = 0
    var hi = _entries - 1
    while (lo < hi) {
      // 找中点
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      // 定位中点的相对offset
      val found = relativeOffset(idx, mid)
      if (found == relOffset)
        // 如果中点就是需要找的，则直接返回
        return mid
      else if (found < relOffset)
        // 中点小于要找的，在[mid, hi]之间找
        lo = mid
      else
        // 中点大于要找的，在[lo, mid-1]之间找
        hi = mid - 1
    }
    lo
  }
  
  /**
    * return the nth offset relative to the base offset
    * 返回第n个offset相对于baseOffset的偏移量
    **/
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8)
  
  /**
    * return the nth physical position
    * 返回第n个offset所指向的消息数据在log文件中的物理偏移量
    **/
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8 + 4)
  
  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException("Attempt to fetch the %dth entry from an index of size %d.".format(n, _entries))
      val idx = mmap.duplicate
      OffsetPosition(relativeOffset(idx, n), physical(idx, n))
    }
  }
  
  /**
   * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
    * 向索引文件中添加索引项
   */
  def append(offset: Long, position: Int) {
    /**
      * inLock是个柯里化方法，会先进行加锁，执行完后会解锁
      */
    inLock(lock) { // 加锁
      // 检查是否存满了
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      if (_entries == 0 || offset > _lastOffset) {
        // 当前文件内没有索引项，传入的offset大于最后一个索引项
        debug("Adding index entry %d => %d to %s.".format(offset, position, _file.getName))
        mmap.putInt((offset - baseOffset).toInt)
        mmap.putInt(position)
        _entries += 1
        _lastOffset = offset
        require(_entries * 8 == mmap.position, _entries + " entries but file position in index is " + mmap.position + ".")
      } else {
        throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s."
          .format(offset, _entries, _lastOffset, _file.getAbsolutePath))
      }
    }
  }
  
  /**
   * True iff there are no more slots available in this index
   */
  def isFull: Boolean = _entries >= _maxEntries
  
  /**
   * Truncate the entire index, deleting all entries
   */
  def truncate() = truncateToEntries(0)
  
  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long) {
    // 加锁
    inLock(lock) {
      /**
        * 复制一份mmap副本，副本和原Buffer的数据是共享的
        * 但操作副本的指针量时原Buffer的指针量不变
        */
      val idx = mmap.duplicate
      // 在buffer中找到小于或等于offset的槽位（序号）
      val slot = indexSlotFor(idx, offset)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       *
       * 三种截断情况：
       * 1. 如果没有找到小于offset的索引，删除所有索引；
       * 2. 如果正好找到了offset，则取[0, offset)之间的，其他的删掉；
       * 3. 如果offset不存在，则取[0, offset + 1]之间的，将(offset + 1, end]之间的全删除
       */
      val newEntries =
        // 如果找到的为-1，表示没找到
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset) // 如果slot的相对偏移量就是要找的，表示找到了
          slot
        else
          // 否则取后面一个
          slot + 1
      // 删掉[newEntries, end]之间的索引
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
   */
  private def truncateToEntries(entries: Int) {
    inLock(lock) {
      // 记录_entries
      _entries = entries
      // position到_entries * 8
      mmap.position(_entries * 8)
      // 更新_lastOffset
      _lastOffset = readLastEntry.offset
    }
  }
  
  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize() {
    inLock(lock) {
      resize(_entries * 8)
    }
  }

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   */
  def resize(newSize: Int) {
    inLock(lock) {
      val raf = new RandomAccessFile(_file, "rw")
      val roundedNewSize = roundToExactMultiple(newSize, 8)
      val position = mmap.position
      
      /* Windows won't let us modify the file length while the file is mmapped :-( */
      if (Os.isWindows)
        forceUnmap(mmap)
      try {
        raf.setLength(roundedNewSize)
        mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
        _maxEntries = mmap.limit / 8
        mmap.position(position)
      } finally {
        CoreUtils.swallow(raf.close())
      }
    }
  }
  
  /**
   * Forcefully free the buffer's mmap. We do this only on windows.
   */
  private def forceUnmap(m: MappedByteBuffer) {
    try {
      if(m.isInstanceOf[sun.nio.ch.DirectBuffer])
        (m.asInstanceOf[sun.nio.ch.DirectBuffer]).cleaner().clean()
    } catch {
      case t: Throwable => warn("Error when freeing index buffer", t)
    }
  }
  
  /**
   * Flush the data in the index to disk
   */
  def flush() {
    inLock(lock) {
      mmap.force()
    }
  }
  
  /**
   * Delete this index file
   */
  def delete(): Boolean = {
    info("Deleting index " + _file.getAbsolutePath)
    if (Os.isWindows)
      CoreUtils.swallow(forceUnmap(mmap))
    _file.delete()
  }
  
  /** The number of entries in this index */
  def entries = _entries
  
  /**
   * The number of bytes actually used by this index
   */
  def sizeInBytes() = 8 * _entries
  
  /** Close the index */
  def close() {
    trimToValidSize()
  }
  
  /**
   * Rename the file that backs this offset index
   * @throws IOException if rename fails
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(_file.toPath, f.toPath)
    finally _file = f
  }
  
  /**
   * Do a basic sanity check on this index to detect obvious problems
   * @throws IllegalArgumentException if any problems are found
   */
  def sanityCheck() {
    require(_entries == 0 || lastOffset > baseOffset,
            "Corrupt index found, index file (%s) has non-zero size but the last offset is %d and the base offset is %d"
            .format(_file.getAbsolutePath, lastOffset, baseOffset))
    val len = _file.length()
    require(len % 8 == 0,
            "Index file " + _file.getName + " is corrupt, found " + len +
            " bytes which is not positive or not a multiple of 8.")
  }
  
  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundToExactMultiple(67, 8) == 64
    * 取小于number的最大的8的倍数
   */
  private def roundToExactMultiple(number: Int, factor: Int) = factor * (number / factor)
  
  /**
   * Execute the given function in a lock only if we are running on windows. We do this 
   * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  private def maybeLock[T](lock: Lock)(fun: => T): T = {
    if(Os.isWindows)
      lock.lock()
    try {
      fun
    } finally {
      if(Os.isWindows)
        lock.unlock()
    }
  }
}
