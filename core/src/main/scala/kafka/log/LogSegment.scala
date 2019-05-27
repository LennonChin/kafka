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

import kafka.message._
import kafka.common._
import kafka.utils._
import kafka.server.{LogOffsetMetadata, FetchDataInfo}
import org.apache.kafka.common.errors.CorruptRecordException

import scala.math._
import java.io.{IOException, File}


 /**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The message set containing log entries 用于操作对应日志文件的FileMessageSet对象
 * @param index The offset index 用于操作对应索引文件的OffsetIndex对象
 * @param baseOffset A lower bound on the offsets in this segment LogSegment中第一条消息的offset值
 * @param indexIntervalBytes The approximate number of bytes between entries in the index 索引项之间间隔的最小字节数
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment(val log: FileMessageSet,
                 val index: OffsetIndex,
                 val baseOffset: Long,
                 val indexIntervalBytes: Int,
                 val rollJitterMs: Long,
                 time: Time) extends Logging {

   /**
     * 标识LogSegment对象创建时间，
     * 当调用truncateTo()方法将整个日志文件清空时，会将此字段重置为当前时间。
     * 参与创建新LogSegment的条件判断，在介绍Log类时会详细介绍
     */
  var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
  // 记录自从上次添加索引项之后，在日志文件中累计加入的Message集合的字节数，用于判断下次索引项添加的时机
  private var bytesSinceLastIndexEntry = 0

  def this(dir: File, startOffset: Long, indexIntervalBytes: Int, maxIndexSize: Int, rollJitterMs: Long, time: Time, fileAlreadyExists: Boolean = false, initFileSize: Int = 0, preallocate: Boolean = false) =
    this(new FileMessageSet(file = Log.logFilename(dir, startOffset), fileAlreadyExists = fileAlreadyExists, initFileSize = initFileSize, preallocate = preallocate),
         new OffsetIndex(Log.indexFilename(dir, startOffset), baseOffset = startOffset, maxIndexSize = maxIndexSize),
         startOffset,
         indexIntervalBytes,
         rollJitterMs,
         time)

  /* Return the size in bytes of this log segment */
  def size: Long = log.sizeInBytes()

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * It is assumed this method is being called from within a lock.
    *
    * 追加消息
   *
   * @param offset The first offset in the message set. 表示messages中的第一条消息的offset，如果是压缩消息，则是第一条内层消息的offset
   * @param messages The messages to append. 待追加的消息集合
   */
  @nonthreadsafe
  def append(offset: Long, messages: ByteBufferMessageSet) {
    if (messages.sizeInBytes > 0) {
      trace("Inserting %d bytes at offset %d at position %d".format(messages.sizeInBytes, offset, log.sizeInBytes()))
      // append an entry to the index (if needed)
      // 检查是否满足添加索引项的条件（用于产生稀疏索引，并不是每一条消息都会产生offset记录的，通过这个条件可以达到只有一部分消息记录了offset）
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        // 添加索引
        index.append(offset, log.sizeInBytes())
        // 添加成功后，将bytesSinceLastIndexEntry置为0
        this.bytesSinceLastIndexEntry = 0
      }
      // append the messages
      // 添加消息数据
      log.append(messages)
      // 更新bytesSinceLastIndexEntry，加上添加的消息大小
      this.bytesSinceLastIndexEntry += messages.sizeInBytes
    }
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   *
   * The lowerBound argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate 想要转换的offset
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and 限定目标position的下限值，默认是0
   * when omitted, the search will begin at the position in the offset index.
   *
   * @return The position in the log storing the message with the least offset >= the requested offset or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): OffsetPosition = {
    /**
      * 通过OffsetIndex在index文件中查找offset对应的OffsetPosition，
      * 因为offset是稀疏的，所以此时的offset和position不一定是命中的
      * 但查找到的结果offset和position必然小于目标offset和position
      */
    val mapping = index.lookup(offset)
    /**
      * 通过FileMessageSet在消息数据文件中查找目标offset和position
      */
    log.searchFor(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read 指定读取的起始消息的offset
   * @param maxSize The maximum number of bytes to include in the message set we read 指定读取的最大字节数
   * @param maxOffset An optional maximum offset for the message set we read 指定读取结束的offset，可以为空
   * @param maxPosition The maximum position in the log segment that should be exposed for read 指定读取的最大物理地址，可选参数，默认值是日志文件的大小
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  @threadsafe
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int, maxPosition: Long = size): FetchDataInfo = {
    // 检查参数
    if(maxSize < 0)
      throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

    val logSize = log.sizeInBytes // this may change, need to save a consistent copy
    // 转换startOffset为在消息文件中的物理偏移量
    val startPosition = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    if(startPosition == null)
      return null

    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition.position)

    // if the size is zero, still return a log segment but with zero size
    // 当maxSize为0时，还是会返回一个大小为0的FetchDataInfo对象
    if(maxSize == 0)
      return FetchDataInfo(offsetMetadata, MessageSet.Empty)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    // 计算需要读取的字节数
    val length = maxOffset match { // maxOffset通常是Replica的HighWater，消费者最多只能读到HighWater位置的消息
      case None =>
        // no max offset, just read until the max position
        // 没有maxOffset
        min((maxPosition - startPosition.position).toInt, maxSize)
      case Some(offset) =>
        // there is a max offset, translate it to a file position and use that to calculate the max read size;
        // when the leader of a partition changes, it's possible for the new leader's high watermark to be less than the
        // true high watermark in the previous leader for a short window. In this window, if a consumer fetches on an
        // offset between new leader's high watermark and the log end offset, we want to return an empty response.
        if(offset < startOffset)
          // maxOffset小于startOffset，返回一个大小为0的FetchDataInfo对象
          return FetchDataInfo(offsetMetadata, MessageSet.Empty)
        // 将maxOffset转换为物理偏移量
        val mapping = translateOffset(offset, startPosition.position)
        // 计算读取数据时的结束偏移量
        val endPosition =
          if(mapping == null)
            // 当转换maxOffset得到的物理偏移量为空时，则读到消息日志文件末尾
            logSize // the max offset is off the end of the log, use the end of the file
          else
            // 否则取maxOffset的物理偏移量
            mapping.position
        // 计算读取的字节数
        min(min(maxPosition, endPosition) - startPosition.position, maxSize).toInt
    }

    /**
      * FetchDataInfo的第二个参数是一个MessageSet对象
      * 该对象底层是一个设置了起始位置和长度的FileMessageSet分片对象
      * 并没有读取消息数据到内存中
      */
    FetchDataInfo(offsetMetadata, log.read(startPosition.position, length))
  }

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log and index.
    * 根据日志文件重建索引文件，同时验证日志文件中消息的合法性
   *
   * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
   * is corrupt.
   *
   * @return The number of bytes truncated from the log
   */
  @nonthreadsafe
  def recover(maxMessageSize: Int): Int = {
    /**
      * 清空索引文件
      * 会修改OffsetIndex对象的记录
      *   - 将索引文件中的索引项个数（_entries）置为0
      *   - 将底层MappedByteBuffer对象（mmap）的position置为0
      *   - 将记录的最后一个索引项的offset（_lastOffset）置为baseOffset
      */
    index.truncate()
    // 修改索引文件的大小
    index.resize(index.maxIndexSize)
    // 记录已经通过验证的字节数
    var validBytes = 0
    // 最后一个索引项对应的物理地址
    var lastIndexEntry = 0
    // FileMessageSet的迭代器
    val iter = log.iterator(maxMessageSize)
    // 遍历FileMessageSet中的消息
    try {
      while(iter.hasNext) {
        val entry = iter.next
        // 验证Message是否合法，主要是验证checkSum，不通过时会抛出InvalidMessageException异常
        entry.message.ensureValid()
        // 判断是否符合添加索引的条件
        if(validBytes - lastIndexEntry > indexIntervalBytes) {
          // we need to decompress the message, if required, to get the offset of the first uncompressed message
          // 根据是否是压缩消息来决定写入的startOffset
          val startOffset =
            entry.message.compressionCodec match {
              case NoCompressionCodec =>
                // 不是压缩消息，直接返回消息的offset
                entry.offset
              case _ =>
                // 是压缩消息，需要深层迭代，取压缩消息中第一条消息的offset
                ByteBufferMessageSet.deepIterator(entry).next().offset
          }
          // 添加索引项
          index.append(startOffset, validBytes)
          // 记录lastIndexEntry
          lastIndexEntry = validBytes
        }
        // 更新validBytes
        validBytes += MessageSet.entrySize(entry.message)
      }
    } catch {
      case e: CorruptRecordException =>
        logger.warn("Found invalid messages in log segment %s at byte offset %d: %s.".format(log.file.getAbsolutePath, validBytes, e.getMessage))
    }
    /**
      * 当try块抛出异常时，validBytes必然是小于日志文件的总大小的
      * 此时日志文件可能出现了Message数据损坏
      * 此处的处理时，只要出现Message损坏，则将其与之后所有的消息数据全部抛弃
      */
    // 计算日志文件需要截断的字节数
    val truncated = log.sizeInBytes - validBytes
    // 对日志文件进行截断操作，丢弃后面验证失败的Message
    log.truncateTo(validBytes)
    // 对索引文件进行截断
    index.trimToValidSize()
    // 返回日志文件截断的字节数
    truncated
  }

  override def toString() = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    val mapping = translateOffset(offset)
    if(mapping == null)
      return 0
    index.truncateTo(offset)
    // after truncation, reset and allocate more space for the (new currently  active) index
    index.resize(index.maxIndexSize)
    val bytesTruncated = log.truncateTo(mapping.position)
    if(log.sizeInBytes == 0)
      created = time.milliseconds
    bytesSinceLastIndexEntry = 0
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def nextOffset(): Long = {
    // 读取得到FetchDataInfo
    val ms = read(index.lastOffset, None, log.sizeInBytes)
    if(ms == null) {
      // 没读到消息，则直接取baseOffset
      baseOffset
    } else {
      // 读到了消息
      ms.messageSet.lastOption match {
          // 读到的消息是空
        case None => baseOffset
          // 读到消息不为空，取最后一个消息的nextOffset
        case Some(last) => last.nextOffset
      }
    }
  }

  /**
   * Flush this log segment to disk
   */
  @threadsafe
  def flush() {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      index.flush()
    }
  }

  /**
   * Change the suffix for the index and log file for this log segment
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String) {

    def kafkaStorageException(fileType: String, e: IOException) =
      new KafkaStorageException(s"Failed to change the $fileType file suffix from $oldSuffix to $newSuffix for log segment $baseOffset", e)

    try log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("log", e)
    }
    try index.renameTo(new File(CoreUtils.replaceSuffix(index.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("index", e)
    }
  }

  /**
   * Close this log segment
   */
  def close() {
    CoreUtils.swallow(index.close)
    CoreUtils.swallow(log.close)
  }

  /**
   * Delete this log segment from the filesystem.
   * @throws KafkaStorageException if the delete fails.
   */
  def delete() {
    // 删除Log日志文件
    val deletedLog = log.delete()
    // 杀出Index索引文件
    val deletedIndex = index.delete()
    // 检查是否删除成功
    if(!deletedLog && log.file.exists)
      throw new KafkaStorageException("Delete of log " + log.file.getName + " failed.")
    if(!deletedIndex && index.file.exists)
      throw new KafkaStorageException("Delete of index " + index.file.getName + " failed.")
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    log.file.setLastModified(ms)
    index.file.setLastModified(ms)
  }
}
