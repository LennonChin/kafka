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

import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic._

import kafka.utils._
import kafka.message._
import kafka.common.KafkaException
import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.network.TransportLayer
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.ArrayBuffer

/**
 * An on-disk message set. An optional start and end position can be applied to the message set
 * which will allow slicing a subset of the file.
 * @param file The file name for the underlying log data 指向磁盘上对应的日志文件
 * @param channel the underlying file channel used 用于读写对应的日志文件
 * @param start A lower bound on the absolute position in the file from which the message set begins 在表示日志文件分片时，分片的起始位置偏移量
 * @param end The upper bound on the absolute position in the file at which the message set ends 在表示日志文件分片时，分片的结束位置偏移量
 * @param isSlice Should the start and end parameters be used for slicing? 是否是日志分片文件
 */
@nonthreadsafe
class FileMessageSet private[kafka](@volatile var file: File,
                                    private[log] val channel: FileChannel,
                                    private[log] val start: Int,
                                    private[log] val end: Int,
                                    isSlice: Boolean) extends MessageSet with Logging {

  /**
    * the size of the message set in bytes
    * 消息文件的大小，单位为字节
    * 因为可能有多个Handler线程并发向同一个分区写入消息，_size是AtomicInteger类型。
    **/
  private val _size =
    if(isSlice)
      // 如果是分片，即为end - start，不对文件大小进行检查
      new AtomicInteger(end - start) // don't check the file size if this is just a slice view
    else
      // 否则需要对文件大小进行检查
      new AtomicInteger(math.min(channel.size.toInt, end) - start)

  /* if this is not a slice, update the file pointer to the end of the file */
  if (!isSlice)
    /**
      * set the file position to the last byte in the file
      * 如果不是分片，则将FileChannel的position置为文件末尾，即从文件末尾开始写入数据
      * 会检查文件大小，取文件大小和end参数的最小值
      **/
    channel.position(math.min(channel.size.toInt, end))

  /**
   * Create a file message set with no slicing.
   */
  def this(file: File, channel: FileChannel) =
    this(file, channel, start = 0, end = Int.MaxValue, isSlice = false)

  /**
   * Create a file message set with no slicing
   */
  def this(file: File) =
    this(file, FileMessageSet.openChannel(file, mutable = true))

  /**
    * Create a file message set with no slicing, and with initFileSize and preallocate.
    * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
    * with one value (for example 512 * 1024 *1024 ) can improve the kafka produce performance.
    * If it's new file and preallocate is true, end will be set to 0.  Otherwise set to Int.MaxValue.
    * @param file 文件
    * @param fileAlreadyExists 文件是否存在
    * @param initFileSize 初始化文件大小
    * @param preallocate 是否采用预分配
    */
  def this(file: File, fileAlreadyExists: Boolean, initFileSize: Int, preallocate: Boolean) =
      // 调用重载构造方法
      this(file,
        channel = FileMessageSet.openChannel(file, mutable = true, fileAlreadyExists, initFileSize, preallocate),
        start = 0,
        // 当是新创建的文件，且采用了预分配时，end会被置为0，因此当该文件开始写入数据时，其实是从开头开始写入的
        end = ( if ( !fileAlreadyExists && preallocate ) 0 else Int.MaxValue),
        isSlice = false)

  /**
   * Create a file message set with mutable option
   */
  def this(file: File, mutable: Boolean) = this(file, FileMessageSet.openChannel(file, mutable))

  /**
   * Create a slice view of the file message set that begins and ends at the given byte offsets
   */
  def this(file: File, channel: FileChannel, start: Int, end: Int) =
    this(file, channel, start, end, isSlice = true)

  /**
   * Return a message set which is a view into this set starting from the given position and with the given size limit.
   *
   * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
   *
   * If this message set is already sliced, the position will be taken relative to that slicing.
   *
   * @param position The start position to begin the read from 读取数据的起始偏移量
   * @param size The number of bytes after the start position to include 读取数据的大小
   *
   * @return A sliced wrapper on this message set limited based on the given position and size
   */
  def read(position: Int, size: Int): FileMessageSet = {
    // 检查参数
    if(position < 0)
      throw new IllegalArgumentException("Invalid position: " + position)
    if(size < 0)
      throw new IllegalArgumentException("Invalid size: " + size)
    // 返回一个日志文件分片
    new FileMessageSet(file,
                       channel,
                       start = this.start + position,
                       end = math.min(this.start + position + size, sizeInBytes()))
  }

  /**
   * Search forward for the file position of the last offset that is greater than or equal to the target offset
   * and return its physical position. If no such offsets are found, return null.
   * @param targetOffset The offset to search for. 需要查找的偏移量
   * @param startingPosition The starting position in the file to begin searching from. 开始查找的偏移量
   */
  def searchFor(targetOffset: Long, startingPosition: Int): OffsetPosition = {
    var position = startingPosition
    // 创建大小为LogOverHead的ByteBuffer，用于读取LogOverHead
    val buffer = ByteBuffer.allocate(MessageSet.LogOverhead)
    // 当前消息文件的大小
    val size = sizeInBytes()
    // 从position位置开始逐条读取消息
    while(position + MessageSet.LogOverhead < size) {
      // rewind指针，开始向ByteBuffer写入数据
      buffer.rewind()
      channel.read(buffer, position)
      // 没有剩余数据，抛出异常
      if(buffer.hasRemaining)
        throw new IllegalStateException("Failed to read complete buffer for targetOffset %d startPosition %d in %s"
                                        .format(targetOffset, startingPosition, file.getAbsolutePath))
      // rewind指针，开始从ByteBuffer读取数据
      buffer.rewind()
      // 读取偏移量
      val offset = buffer.getLong()
      // 如果偏移量大于等于targetOffset，表示读到了
      if(offset >= targetOffset)
        // 返回OffsetPosition偏移量对象
        return OffsetPosition(offset, position)
      // 获取读取到的消息数据的大小
      val messageSize = buffer.getInt()
      // 如果消息数据大小小于消息最小大小，说明读取出错，抛出异常
      if(messageSize < Message.MinMessageOverhead)
        throw new IllegalStateException("Invalid message size: " + messageSize)
      // 否则后移position，准备下次读取
      position += MessageSet.LogOverhead + messageSize
    }
    // 找不到targetOffset偏移量对应的消息，返回null
    null
  }

  /**
   * Write some of this set to the given channel.
    * 将消息数据写入到指定的其他的FileChannel中
   * @param destChannel The channel to write to.
   * @param writePosition The position in the message set to begin writing from. 写入数据的起始偏移量
   * @param size The maximum number of bytes to write 写入数据的大小
   * @return The number of bytes actually written. 写入数据的字节数
   */
  def writeTo(destChannel: GatheringByteChannel, writePosition: Long, size: Int): Int = {
    // Ensure that the underlying size has not changed.
    // 计算可写数据的长度
    val newSize = math.min(channel.size.toInt, end) - start
    // 当可写数据长度小于消息文件长度，说明可能在写入过程中文件被截断了，抛出异常
    if (newSize < _size.get()) {
      throw new KafkaException("Size of FileMessageSet %s has been truncated during write: old size %d, new size %d"
        .format(file.getAbsolutePath, _size.get(), newSize))
    }
    // 获取写入起始偏移量
    val position = start + writePosition
    // 计算可写数据大小
    val count = math.min(size, sizeInBytes)
    // 进行写入，使用Channel的transfer方法（零拷贝），并记录写入的数据量字节数
    val bytesTransferred = (destChannel match {
      case tl: TransportLayer => tl.transferFrom(channel, position, count)
      case dc => channel.transferTo(position, count, dc)
    }).toInt
    trace("FileMessageSet " + file.getAbsolutePath + " : bytes transferred : " + bytesTransferred
      + " bytes requested for transfer : " + math.min(size, sizeInBytes))
    // 返回写入数据量字节数
    bytesTransferred
  }

  /**
    * This method is called before we write messages to the socket using zero-copy transfer. We need to
    * make sure all the messages in the message set have the expected magic value.
    *
    * @param expectedMagicValue the magic value expected
    * @return true if all messages have expected magic value, false otherwise
    */
  override def isMagicValueInAllWrapperMessages(expectedMagicValue: Byte): Boolean = {
    var location = start
    val offsetAndSizeBuffer = ByteBuffer.allocate(MessageSet.LogOverhead)
    val crcAndMagicByteBuffer = ByteBuffer.allocate(Message.CrcLength + Message.MagicLength)
    while (location < end) {
      offsetAndSizeBuffer.rewind()
      channel.read(offsetAndSizeBuffer, location)
      if (offsetAndSizeBuffer.hasRemaining)
        return true
      offsetAndSizeBuffer.rewind()
      offsetAndSizeBuffer.getLong // skip offset field
      val messageSize = offsetAndSizeBuffer.getInt
      if (messageSize < Message.MinMessageOverhead)
        throw new IllegalStateException("Invalid message size: " + messageSize)
      crcAndMagicByteBuffer.rewind()
      channel.read(crcAndMagicByteBuffer, location + MessageSet.LogOverhead)
      if (crcAndMagicByteBuffer.get(Message.MagicOffset) != expectedMagicValue)
        return false
      location += (MessageSet.LogOverhead + messageSize)
    }
    true
  }

  /**
   * Convert this message set to use the specified message format.
   */
  def toMessageFormat(toMagicValue: Byte): MessageSet = {
    val offsets = new ArrayBuffer[Long]
    val newMessages = new ArrayBuffer[Message]
    this.foreach { messageAndOffset =>
      val message = messageAndOffset.message
      if (message.compressionCodec == NoCompressionCodec) {
        newMessages += message.toFormatVersion(toMagicValue)
        offsets += messageAndOffset.offset
      } else {
        // File message set only has shallow iterator. We need to do deep iteration here if needed.
        val deepIter = ByteBufferMessageSet.deepIterator(messageAndOffset)
        for (innerMessageAndOffset <- deepIter) {
          newMessages += innerMessageAndOffset.message.toFormatVersion(toMagicValue)
          offsets += innerMessageAndOffset.offset
        }
      }
    }

    if (sizeInBytes > 0 && newMessages.size == 0) {
      // This indicates that the message is too large. We just return all the bytes in the file message set.
      this
    } else {
      // We use the offset seq to assign offsets so the offset of the messages does not change.
      new ByteBufferMessageSet(
        compressionCodec = this.headOption.map(_.message.compressionCodec).getOrElse(NoCompressionCodec),
        offsetSeq = offsets,
        newMessages: _*)
    }
  }

  /**
   * Get a shallow iterator over the messages in the set.
   */
  override def iterator: Iterator[MessageAndOffset] = iterator(Int.MaxValue)

  /**
   * Get an iterator over the messages in the set. We only do shallow iteration here.
   * @param maxMessageSize A limit on allowable message size to avoid allocating unbounded memory.
   * If we encounter a message larger than this we throw an InvalidMessageException.
   * @return The iterator.
   */
  def iterator(maxMessageSize: Int): Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      // 起始位置为start
      var location = start
      // LogOverHead大小
      val sizeOffsetLength = 12
      // 装载LogOverHead的ByteBuffer
      val sizeOffsetBuffer = ByteBuffer.allocate(sizeOffsetLength)

      override def makeNext(): MessageAndOffset = {
        // 如果location + sizeOffsetLength >= end，表示读到了末尾
        if(location + sizeOffsetLength >= end)
          // 返回allDone()，内部其实返回了null
          return allDone()

        // read the size of the item
        // 读取LogOverHead
        sizeOffsetBuffer.rewind()
        // 从Channel读取数据到sizeOffsetBuffer，从location开始读取
        channel.read(sizeOffsetBuffer, location)
        // 如果没有读满sizeOffsetBuffer，表示读到了末尾
        if(sizeOffsetBuffer.hasRemaining)
        // 返回allDone()，内部其实返回了null
          return allDone()

        sizeOffsetBuffer.rewind()
        // 获取offset
        val offset = sizeOffsetBuffer.getLong()
        // 获取size
        val size = sizeOffsetBuffer.getInt()
        // 检查size
        if(size < Message.MinMessageOverhead || location + sizeOffsetLength + size > end)
          return allDone()
        if(size > maxMessageSize)
          throw new CorruptRecordException("Message size exceeds the largest allowable message size (%d).".format(maxMessageSize))

        // read the item itself
        // 按照size分配指定大小的ByteBuffer
        val buffer = ByteBuffer.allocate(size)
        // 从Channel中读取数据到buffer
        channel.read(buffer, location + sizeOffsetLength)
        // 如果没有读满buffer，表示读到了末尾
        if(buffer.hasRemaining)
          return allDone()
        buffer.rewind()

        // increment the location and return the item
        // 维护location
        location += size + sizeOffsetLength
        // 返回MessageAndOffset消息封装对象
        new MessageAndOffset(new Message(buffer), offset)
      }
    }
  }

  /**
   * The number of bytes taken up by this file set
   */
  def sizeInBytes(): Int = _size.get()

  /**
   * Append these messages to the message set
   */
  def append(messages: ByteBufferMessageSet) {
    // 写文件
    val written = messages.writeFullyTo(channel)
    // 增加消息文件的大小记录值
    _size.getAndAdd(written)
  }

  /**
   * Commit all written data to the physical disk
   */
  def flush() = {
    channel.force(true)
  }

  /**
   * Close this message set
   */
  def close() {
    flush()
    trim()
    channel.close()
  }

  /**
   * Trim file when close or roll to next file
   */
  def trim() {
    truncateTo(sizeInBytes())
  }

  /**
   * Delete this message set from the filesystem
    * 删除文件
   * @return True iff this message set was deleted.
   */
  def delete(): Boolean = {
    CoreUtils.swallow(channel.close())
    // 删除文件
    file.delete()
  }

  /**
   * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
   * given size falls on a valid message boundary.
   * In some versions of the JDK truncating to the same size as the file message set will cause an
   * update of the files mtime, so truncate is only performed if the targetSize is smaller than the
   * size of the underlying FileChannel.
   * It is expected that no other threads will do writes to the log when this function is called.
   * @param targetSize The size to truncate to. Must be between 0 and sizeInBytes. 裁剪的目标大小
   * @return The number of bytes truncated off 裁剪掉的字节数
   */
  def truncateTo(targetSize: Int): Int = {
    // 获取日志文件原始大小
    val originalSize = sizeInBytes
    // 检查targetSize的有效性
    if(targetSize > originalSize || targetSize < 0)
      throw new KafkaException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                               " size of this log segment is " + originalSize + " bytes.")
    if (targetSize < channel.size.toInt) {
      // 裁剪文件
      channel.truncate(targetSize)
      // 移动position
      channel.position(targetSize)
      // 修改_size
      _size.set(targetSize)
    }
    // 返回裁剪掉的字节数
    originalSize - targetSize
  }

  /**
   * Read from the underlying file into the buffer starting at the given position
   */
  def readInto(buffer: ByteBuffer, relativePosition: Int): ByteBuffer = {
    channel.read(buffer, relativePosition + this.start)
    buffer.flip()
    buffer
  }

  /**
   * Rename the file that backs this message set
   * @throws IOException if rename fails.
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    finally this.file = f
  }

}

object FileMessageSet
{
  /**
   * Open a channel for the given file
   * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
   * with one value (for example 512 * 1025 *1024 ) can improve the kafka produce performance.
   * @param file File path
   * @param mutable mutable
   * @param fileAlreadyExists File already exists or not
   * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
   * @param preallocate Pre allocate file or not, gotten from configuration.
   */
  def openChannel(file: File, mutable: Boolean, fileAlreadyExists: Boolean = false, initFileSize: Int = 0, preallocate: Boolean = false): FileChannel = {
    // 根据mutable参数决定文件是否可写
    if (mutable) {
      if (fileAlreadyExists)
        // 如果文件已经存在，构建RandomAccessFile对象，获取其channel并返回
        new RandomAccessFile(file, "rw").getChannel()
      else {
        // 否则根据preallocate来决定如何创建
        if (preallocate) {
          // 进行文件预分配
          val randomAccessFile = new RandomAccessFile(file, "rw")
          randomAccessFile.setLength(initFileSize)
          randomAccessFile.getChannel()
        }
        else
          // 创建可读写的FileChannel并返回
          new RandomAccessFile(file, "rw").getChannel()
      }
    }
    else
      // 创建只读的FileChannel并返回
      new FileInputStream(file).getChannel()
  }
}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
