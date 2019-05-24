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

package kafka.message

import kafka.utils.{CoreUtils, IteratorTemplate, Logging}
import kafka.common.{KafkaException, LongRef}
import java.nio.ByteBuffer
import java.nio.channels._
import java.io._
import java.util.ArrayDeque

import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable

object ByteBufferMessageSet {

  private def create(offsetAssigner: OffsetAssigner,
                     compressionCodec: CompressionCodec,
                     wrapperMessageTimestamp: Option[Long],
                     timestampType: TimestampType,
                     messages: Message*): ByteBuffer = {
    // 如果传入的消息为空，则返回空的ByteBuffer
    if (messages.isEmpty)
      MessageSet.Empty.buffer
    else if (compressionCodec == NoCompressionCodec) {// 无压缩器
      // 计算并创建大小可容纳所有传入消息的ByteBuffer
      val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      // 遍历所有传入的消息，并将数据写入到buffer
      for (message <- messages) writeMessage(buffer, message, offsetAssigner.nextAbsoluteOffset())
      buffer.rewind()
      // 返回buffer
      buffer
    } else {
      // 需要压缩
      // 得到魔数和时间戳
      val magicAndTimestamp = wrapperMessageTimestamp match {
        case Some(ts) => MagicAndTimestamp(messages.head.magic, ts)
        case None => MessageSet.magicAndLargestTimestamp(messages)
      }

      var offset = -1L
      // 底层使用byte数组保存写入的压缩数据
      val messageWriter = new MessageWriter(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
      // write是个柯里化方法
      messageWriter.write(codec = compressionCodec, timestamp = magicAndTimestamp.timestamp, timestampType = timestampType, magicValue = magicAndTimestamp.magic) { outputStream =>
        // 创建指定压缩类型的输入流
        val output = new DataOutputStream(CompressionFactory(compressionCodec, magicAndTimestamp.magic, outputStream))
        try {
          // 遍历写入内层压缩消息
          for (message <- messages) {
            offset = offsetAssigner.nextAbsoluteOffset()
            if (message.magic != magicAndTimestamp.magic)
              throw new IllegalArgumentException("Messages in the message set must have same magic value")
            // Use inner offset if magic value is greater than 0
            // 魔数为1.写入的是相对offset；魔数为0，写的是offset
            if (magicAndTimestamp.magic > Message.MagicValue_V0)
              output.writeLong(offsetAssigner.toInnerOffset(offset))
            else
              output.writeLong(offset)
            // 写入size
            output.writeInt(message.size)
            // 写入Message数据
            output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
          }
        } finally {
          output.close()
        }
      }
      val buffer = ByteBuffer.allocate(messageWriter.size + MessageSet.LogOverhead)
      // 按照消息格式写入整个外层消息，外层消息的offset是最后一条内层消息的offset
      writeMessage(buffer, messageWriter, offset)
      buffer.rewind()
      buffer
    }
  }

  /** Deep iterator that decompresses the message sets and adjusts timestamp and offset if needed. */
  def deepIterator(wrapperMessageAndOffset: MessageAndOffset): Iterator[MessageAndOffset] = {

    import Message._

    new IteratorTemplate[MessageAndOffset] {

      val MessageAndOffset(wrapperMessage, wrapperMessageOffset) = wrapperMessageAndOffset

      if (wrapperMessage.payload == null)
        throw new KafkaException(s"Message payload is null: $wrapperMessage")

      val wrapperMessageTimestampOpt: Option[Long] =
        if (wrapperMessage.magic > MagicValue_V0) Some(wrapperMessage.timestamp) else None
      val wrapperMessageTimestampTypeOpt: Option[TimestampType] =
        if (wrapperMessage.magic > MagicValue_V0) Some(wrapperMessage.timestampType) else None

      var lastInnerOffset = -1L

      val messageAndOffsets = {
        val inputStream = new ByteBufferBackedInputStream(wrapperMessage.payload)
        val compressed = try {
          new DataInputStream(CompressionFactory(wrapperMessage.compressionCodec, wrapperMessage.magic, inputStream))
        } catch {
          case ioe: IOException =>
            throw new InvalidMessageException(s"Failed to instantiate input stream compressed with ${wrapperMessage.compressionCodec}", ioe)
        }

        val innerMessageAndOffsets = new ArrayDeque[MessageAndOffset]()
        try {
          while (true)
            innerMessageAndOffsets.add(readMessageFromStream(compressed))
        } catch {
          case eofe: EOFException =>
            // we don't do anything at all here, because the finally
            // will close the compressed input stream, and we simply
            // want to return the innerMessageAndOffsets
          case ioe: IOException =>
            throw new InvalidMessageException(s"Error while reading message from stream compressed with ${wrapperMessage.compressionCodec}", ioe)
        } finally {
          CoreUtils.swallow(compressed.close())
        }

        innerMessageAndOffsets
      }

      private def readMessageFromStream(compressed: DataInputStream): MessageAndOffset = {
        val innerOffset = compressed.readLong()
        val recordSize = compressed.readInt()

        if (recordSize < MinMessageOverhead)
          throw new InvalidMessageException(s"Message found with corrupt size `$recordSize` in deep iterator")

        // read the record into an intermediate record buffer (i.e. extra copy needed)
        val bufferArray = new Array[Byte](recordSize)
        compressed.readFully(bufferArray, 0, recordSize)
        val buffer = ByteBuffer.wrap(bufferArray)

        // Override the timestamp if necessary
        val newMessage = new Message(buffer, wrapperMessageTimestampOpt, wrapperMessageTimestampTypeOpt)

        // Inner message and wrapper message must have same magic value
        if (newMessage.magic != wrapperMessage.magic)
          throw new IllegalStateException(s"Compressed message has magic value ${wrapperMessage.magic} " +
            s"but inner message has magic value ${newMessage.magic}")
        lastInnerOffset = innerOffset
        new MessageAndOffset(newMessage, innerOffset)
      }

      override def makeNext(): MessageAndOffset = {
        messageAndOffsets.pollFirst() match {
          case null => allDone()
          case nextMessage@ MessageAndOffset(message, offset) =>
            if (wrapperMessage.magic > MagicValue_V0) {
              val relativeOffset = offset - lastInnerOffset
              val absoluteOffset = wrapperMessageOffset + relativeOffset
              new MessageAndOffset(message, absoluteOffset)
            } else {
              nextMessage
            }
        }
      }
    }
  }

  // 将消息数据写入到buffer中
  private[kafka] def writeMessage(buffer: ByteBuffer, message: Message, offset: Long) {
    // 先写入offset
    buffer.putLong(offset)
    // 再写入消息大小
    buffer.putInt(message.size)
    // 再写入消息数据
    buffer.put(message.buffer)
    message.buffer.rewind()
  }

  private[kafka] def writeMessage(buffer: ByteBuffer, messageWriter: MessageWriter, offset: Long) {
    buffer.putLong(offset)
    buffer.putInt(messageWriter.size)
    messageWriter.writeTo(buffer)
  }
}

private object OffsetAssigner {

  def apply(offsetCounter: LongRef, size: Int): OffsetAssigner =
    new OffsetAssigner(offsetCounter.value to offsetCounter.addAndGet(size))

}

private class OffsetAssigner(offsets: Seq[Long]) {
  private var index = 0

  def nextAbsoluteOffset(): Long = {
    val result = offsets(index)
    index += 1
    result
  }

  def toInnerOffset(offset: Long): Long = offset - offsets.head

}

/**
 * A sequence of messages stored in a byte buffer
 *
 * There are two ways to create a ByteBufferMessageSet
 *
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 *
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 *
 *
 * Message format v1 has the following changes:
 * - For non-compressed messages, timestamp and timestamp type attributes have been added. The offsets of
 *   the messages remain absolute offsets.
 * - For compressed messages, timestamp and timestamp type attributes have been added and inner offsets (IO) are used
 *   for inner messages of compressed messages (see offset calculation details below). The timestamp type
 *   attribute is only set in wrapper messages. Inner messages always have CreateTime as the timestamp type in attributes.
 *
 * We set the timestamp in the following way:
 * For non-compressed messages: the timestamp and timestamp type message attributes are set and used.
 * For compressed messages:
 * 1. Wrapper messages' timestamp type attribute is set to the proper value
 * 2. Wrapper messages' timestamp is set to:
 *    - the max timestamp of inner messages if CreateTime is used
 *    - the current server time if wrapper message's timestamp = LogAppendTime.
 *      In this case the wrapper message timestamp is used and all the timestamps of inner messages are ignored.
 * 3. Inner messages' timestamp will be:
 *    - used when wrapper message's timestamp type is CreateTime
 *    - ignored when wrapper message's timestamp type is LogAppendTime
 * 4. Inner messages' timestamp type will always be ignored with one exception: producers must set the inner message
 *    timestamp type to CreateTime, otherwise the messages will be rejected by broker.
 *
 * Absolute offsets are calculated in the following way:
 * Ideally the conversion from relative offset(RO) to absolute offset(AO) should be:
 *
 * AO = AO_Of_Last_Inner_Message + RO
 *
 * However, note that the message sets sent by producers are compressed in a streaming way.
 * And the relative offset of an inner message compared with the last inner message is not known until
 * the last inner message is written.
 * Unfortunately we are not able to change the previously written messages after the last message is written to
 * the message set when stream compression is used.
 *
 * To solve this issue, we use the following solution:
 *
 * 1. When the producer creates a message set, it simply writes all the messages into a compressed message set with
 *    offset 0, 1, ... (inner offset).
 * 2. The broker will set the offset of the wrapper message to the absolute offset of the last message in the
 *    message set.
 * 3. When a consumer sees the message set, it first decompresses the entire message set to find out the inner
 *    offset (IO) of the last inner message. Then it computes RO and AO of previous messages:
 *
 *    RO = IO_of_a_message - IO_of_the_last_message
 *    AO = AO_Of_Last_Inner_Message + RO
 *
 * 4. This solution works for compacted message sets as well.
 *
 */
class ByteBufferMessageSet(val buffer: ByteBuffer) extends MessageSet with Logging {
  private var shallowValidByteCount = -1

  private[kafka] def this(compressionCodec: CompressionCodec,
                          offsetCounter: LongRef,
                          wrapperMessageTimestamp: Option[Long],
                          timestampType: TimestampType,
                          messages: Message*) {
    this(ByteBufferMessageSet.create(OffsetAssigner(offsetCounter, messages.size), compressionCodec,
      wrapperMessageTimestamp, timestampType, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, offsetCounter: LongRef, messages: Message*) {
    this(compressionCodec, offsetCounter, None, TimestampType.CREATE_TIME, messages:_*)
  }

  def this(compressionCodec: CompressionCodec, offsetSeq: Seq[Long], messages: Message*) {
    this(ByteBufferMessageSet.create(new OffsetAssigner(offsetSeq), compressionCodec,
      None, TimestampType.CREATE_TIME, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(compressionCodec, new LongRef(0L), messages: _*)
  }

  def this(messages: Message*) {
    this(NoCompressionCodec, messages: _*)
  }

  def getBuffer = buffer

  private def shallowValidBytes: Int = {
    if (shallowValidByteCount < 0) {
      // 说明是浅层消息，计算所有浅层消息的载荷大小
      this.shallowValidByteCount = this.internalIterator(isShallow = true).map { messageAndOffset =>
        MessageSet.entrySize(messageAndOffset.message)
      }.sum
    }
    // 否则是压缩的，有深层消息
    shallowValidByteCount
  }

  /** Write the messages in this set to the given channel */
  def writeFullyTo(channel: GatheringByteChannel): Int = {
    // mark一下
    buffer.mark()
    var written = 0
    // 将ByteBufferMessageSet中的数据全部写入文件，并记录写入的字节数
    while (written < sizeInBytes)
      written += channel.write(buffer)
    // reset
    buffer.reset()
    // 返回写入的字节数
    written
  }

  /** Write the messages in this set to the given channel starting at the given offset byte.
    * Less than the complete amount may be written, but no more than maxSize can be. The number
    * of bytes written is returned */
  def writeTo(channel: GatheringByteChannel, offset: Long, maxSize: Int): Int = {
    if (offset > Int.MaxValue)
      throw new IllegalArgumentException(s"offset should not be larger than Int.MaxValue: $offset")
    val dup = buffer.duplicate()
    val position = offset.toInt
    dup.position(position)
    dup.limit(math.min(buffer.limit, position + maxSize))
    channel.write(dup)
  }

  override def isMagicValueInAllWrapperMessages(expectedMagicValue: Byte): Boolean = {
    for (messageAndOffset <- shallowIterator) {
      if (messageAndOffset.message.magic != expectedMagicValue)
        return false
    }
    true
  }

  /** default iterator that iterates over decompressed messages */
  override def iterator: Iterator[MessageAndOffset] = internalIterator()

  /** iterator over compressed messages without decompressing */
  def shallowIterator: Iterator[MessageAndOffset] = internalIterator(true)

  /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. **/
  private def internalIterator(isShallow: Boolean = false): Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      var topIter = buffer.slice()
      var innerIter: Iterator[MessageAndOffset] = null

      def innerDone(): Boolean = (innerIter == null || !innerIter.hasNext)

      def makeNextOuter: MessageAndOffset = {
        // if there isn't at least an offset and size, we are done
        if (topIter.remaining < 12)
          return allDone()
        val offset = topIter.getLong()
        val size = topIter.getInt()
        if(size < Message.MinMessageOverhead)
          throw new InvalidMessageException("Message found with corrupt size (" + size + ") in shallow iterator")

        // we have an incomplete message
        if(topIter.remaining < size)
          return allDone()

        // read the current message and check correctness
        val message = topIter.slice()
        message.limit(size)
        topIter.position(topIter.position + size)
        val newMessage = new Message(message)
        if(isShallow) {
          // 浅层迭代
          new MessageAndOffset(newMessage, offset)
        } else {
          // 深层迭代
          newMessage.compressionCodec match {
            case NoCompressionCodec =>
              innerIter = null
              new MessageAndOffset(newMessage, offset)
            case _ =>
              innerIter = ByteBufferMessageSet.deepIterator(new MessageAndOffset(newMessage, offset))
              if(!innerIter.hasNext)
                innerIter = null
              makeNext()
          }
        }
      }

      override def makeNext(): MessageAndOffset = {
        if(isShallow){
          makeNextOuter
        } else {
          if(innerDone())
            makeNextOuter
          else
            innerIter.next()
        }
      }

    }
  }

  /**
   * Update the offsets for this message set and do further validation on messages including:
    * 更新集合中Message的offset，同时验证消息
   * 1. Messages for compacted topics must have keys 压缩的消息必须有键
   * 2. When magic value = 1, inner messages of a compressed message set must have monotonically increasing offsets
   *    starting from 0. 如果魔数为1，内层压缩消息的offset必须是从offset单调递增的
   * 3. When magic value = 1, validate and maybe overwrite timestamps of messages. 当魔数为1，验证时间戳，同时在必要条件下重写时间戳
   *
   * This method will convert the messages in the following scenarios:
    * 在下面的两类情况下会进行消息转换（即消息魔数与messageFormatVersion不一致时）
   * A. Magic value of a message = 0 and messageFormatVersion is 1 魔数为0，messageFormatVersion为1
   * B. Magic value of a message = 1 and messageFormatVersion is 0 模式为1，messageFormatVersion为0
   *
   * If no format conversion or value overwriting is required for messages, this method will perform in-place
   * operations and avoid re-compression.
    * 如果不需要进行消息格式转换，且不需要重写消息的部分数据，将会复用当前的ByteBufferMessageSet以避免重新压缩
   *
   * Returns the message set and a boolean indicating whether the message sizes may have changed.
    * 用于验证消息并分配offset
   */
  private[kafka] def validateMessagesAndAssignOffsets(offsetCounter: LongRef,
                                                      now: Long,
                                                      sourceCodec: CompressionCodec,
                                                      targetCodec: CompressionCodec,
                                                      compactedTopic: Boolean = false,
                                                      messageFormatVersion: Byte = Message.CurrentMagicValue,
                                                      messageTimestampType: TimestampType,
                                                      messageTimestampDiffMaxMs: Long): (ByteBufferMessageSet, Boolean) = {
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      // check the magic value
      // 检查所有Message的魔数与指定的魔数是否一致
      if (!isMagicValueInAllWrapperMessages(messageFormatVersion)) {
        // Message format conversion
        /**
          * 因为存在Message的魔数不一致问题，需要进行统一，这可能会导致消息总长度变化，需要创建新的ByteBufferMessageSet。
          * 同时还会进行offset的分配，验证并更新CRC32、时间戳等信息
          */
        (convertNonCompressedMessages(offsetCounter, compactedTopic, now, messageTimestampType, messageTimestampDiffMaxMs,
          messageFormatVersion), true)
      } else {
        // Do in-place validation, offset assignment and maybe set timestamp
        /**
          * 处理非压缩消息且魔数值统一的情况，由于魔数确定，长度不会发生改变。
          * 主要是进行offset分配，验证并更新CRC32、时间戳等信息。
          */
        (validateNonCompressedMessagesAndAssignOffsetInPlace(offsetCounter, now, compactedTopic, messageTimestampType,
          messageTimestampDiffMaxMs), false)
      }
    } else {
      /**
        * 处理压缩消息的情况。
        * inPlaceAssignment标识是否可以直接复用当前ByteBufferMessage对象。
        * 下面4种情况不能复用：
        * 1. 消息当前压缩类型与此broker指定的压缩类型不一致，需要重新压缩
        * 2. 魔数为0时，需要重写消息的offset为绝对的offset
        * 3. 魔数大于0，但内部压缩消息某些字段需要修改，例如时间戳
        * 4. 消息格式需要转换
        */
      // Deal with compressed messages
      // We cannot do in place assignment in one of the following situations:
      // 1. Source and target compression codec are different
      // 2. When magic value to use is 0 because offsets need to be overwritten
      // 3. When magic value to use is above 0, but some fields of inner messages need to be overwritten.
      // 4. Message format conversion is needed.

      // No in place assignment situation 1 and 2
      var inPlaceAssignment = sourceCodec == targetCodec && // 检查情况1
        messageFormatVersion > Message.MagicValue_V0 // 检查情况2

      var maxTimestamp = Message.NoTimestamp
      val expectedInnerOffset = new LongRef(0)
      val validatedMessages = new mutable.ArrayBuffer[Message]
      // 遍历内层压缩消息，此步骤会解压
      this.internalIterator(isShallow = false).foreach { messageAndOffset =>
        val message = messageAndOffset.message
        // 检查消息的键
        validateMessageKey(message, compactedTopic)

        if (message.magic > Message.MagicValue_V0 && messageFormatVersion > Message.MagicValue_V0) {
          // No in place assignment situation 3
          // Validate the timestamp
          // 检查时间戳
          validateTimestamp(message, now, messageTimestampType, messageTimestampDiffMaxMs)
          // Check if we need to overwrite offset
          // 检查情况3，检查内部offset是否正常
          if (messageAndOffset.offset != expectedInnerOffset.getAndIncrement())
            inPlaceAssignment = false
          maxTimestamp = math.max(maxTimestamp, message.timestamp)
        }

        if (sourceCodec != NoCompressionCodec && message.compressionCodec != NoCompressionCodec)
          throw new InvalidMessageException("Compressed outer message should not have an inner message with a " +
            s"compression attribute set: $message")

        // No in place assignment situation 4
        // 检查情况4
        if (message.magic != messageFormatVersion)
          inPlaceAssignment = false

        // 保存通过上述检测和转换的Message的集合
        validatedMessages += message.toFormatVersion(messageFormatVersion)
      }

      if (!inPlaceAssignment) {
        // 不能复用当前的ByteBufferMessage对象的场景
        // Cannot do in place assignment.
        val wrapperMessageTimestamp = {
          if (messageFormatVersion == Message.MagicValue_V0)
            Some(Message.NoTimestamp)
          else if (messageFormatVersion > Message.MagicValue_V0 && messageTimestampType == TimestampType.CREATE_TIME)
            Some(maxTimestamp)
          else // Log append time
            Some(now)
        }

        // 创建新ByteBufferMessageSet对象，重新压缩。此时调用上面介绍的create()方法进行压缩
        (new ByteBufferMessageSet(compressionCodec = targetCodec,
                                  offsetCounter = offsetCounter,
                                  wrapperMessageTimestamp = wrapperMessageTimestamp,
                                  timestampType = messageTimestampType,
                                  messages = validatedMessages: _*), true)
      } else {
        // 复用当前ByteBufferMessageSet对象，这样可以减少一次压缩操作
        // Do not do re-compression but simply update the offset, timestamp and attributes field of the wrapper message.
        // 更新外层消息的offset，将其offset更新为内部最后一条压缩消息的offset
        buffer.putLong(0, offsetCounter.addAndGet(validatedMessages.size) - 1)
        // validate the messages
        validatedMessages.foreach(_.ensureValid())

        var crcUpdateNeeded = true
        val timestampOffset = MessageSet.LogOverhead + Message.TimestampOffset
        val attributeOffset = MessageSet.LogOverhead + Message.AttributesOffset
        val timestamp = buffer.getLong(timestampOffset)
        val attributes = buffer.get(attributeOffset)
        if (messageTimestampType == TimestampType.CREATE_TIME && timestamp == maxTimestamp)
          // We don't need to recompute crc if the timestamp is not updated.
          crcUpdateNeeded = false
        else if (messageTimestampType == TimestampType.LOG_APPEND_TIME) {
          // Set timestamp type and timestamp
          // 更新外层消息的时间戳
          buffer.putLong(timestampOffset, now)
          // 更新外层消息的attribute
          buffer.put(attributeOffset, messageTimestampType.updateAttributes(attributes))
        }

        if (crcUpdateNeeded) {
          // need to recompute the crc value
          buffer.position(MessageSet.LogOverhead)
          val wrapperMessage = new Message(buffer.slice())
          // 更新外层消息的CRC32
          Utils.writeUnsignedInt(buffer, MessageSet.LogOverhead + Message.CrcOffset, wrapperMessage.computeChecksum)
        }
        buffer.rewind()
        (this, false)
      }
    }
  }

  // We create this method to avoid a memory copy. It reads from the original message set and directly
  // writes the converted messages into new message set buffer. Hence we don't need to allocate memory for each
  // individual message during message format conversion.
  /**
    * Message的魔数不一致问题，需要进行统一，这可能会导致消息总长度变化，需要创建新的ByteBufferMessageSet。
    * 同时还会进行offset的分配，验证并更新CRC32、时间戳等信息。
    * @param offsetCounter offset自增器
    * @param compactedTopic 是否是压缩主题
    * @param now 当前时间，用于在时间戳类型为CREATE_TIME时验证消息时间戳
    * @param timestampType 时间戳类型
    * @param messageTimestampDiffMaxMs
    * @param toMagicValue 转换后的魔数
    * @return 转换后的ByteBufferMessageSet对象
    *
    * ByteBufferMessageSet中单条消息格式
    * |------- | ---- | ----- | ------|------------|-----------|----------|-----|------------|------ |
    * | offset | size | CRC32 | magic | attributes | timestamp | key size | key | value size | value |
    * |------- | ---- | ----- | ------|------------|-----------|----------|-----|------------|------ |
    */
  private def convertNonCompressedMessages(offsetCounter: LongRef,
                                           compactedTopic: Boolean,
                                           now: Long,
                                           timestampType: TimestampType,
                                           messageTimestampDiffMaxMs: Long,
                                           toMagicValue: Byte): ByteBufferMessageSet = {
    // 计算转换后需要的字节大小
    val sizeInBytesAfterConversion = shallowValidBytes + // 所有消息的载荷大小
      this.internalIterator(isShallow = true).map { messageAndOffset =>
      Message.headerSizeDiff(messageAndOffset.message.magic, toMagicValue)
    }.sum // 所有消息头信息的大小
    // 根据转换后的字节大小创建ByteBuffer
    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion)
    var newMessagePosition = 0
    // 因为无压缩，直接浅层迭代消息
    this.internalIterator(isShallow = true).foreach { case MessageAndOffset(message, _) =>
      // 验证消息的键，如果是压缩主题，消息必须要有键
      validateMessageKey(message, compactedTopic)
      // 验证时间戳
      validateTimestamp(message, now, timestampType, messageTimestampDiffMaxMs)
      newBuffer.position(newMessagePosition)
      // 写入偏移量
      newBuffer.putLong(offsetCounter.getAndIncrement())
      // 新的消息大小 = 消息数据大小 + 消息头信息大小
      val newMessageSize = message.size + Message.headerSizeDiff(message.magic, toMagicValue)
      // 写入新的消息大小
      newBuffer.putInt(newMessageSize)
      // 从当前的position位置开始创建缓冲区副本
      val newMessageBuffer = newBuffer.slice()
      // 重设limit为新消息大小，此时position ~ limit之间的空间是用于装载消息数据的
      newMessageBuffer.limit(newMessageSize)
      // 转换消息并写入到newMessageBuffer缓冲区
      message.convertToBuffer(toMagicValue, newMessageBuffer, now, timestampType)
      // 更新position
      newMessagePosition += MessageSet.LogOverhead + newMessageSize
    }
    // position置0
    newBuffer.rewind()
    // 返回以newBuffer创建ByteBufferMessageSet对象
    new ByteBufferMessageSet(newBuffer)
  }

  /**
    * 验证非压缩消息并分配offset
    * ByteBufferMessageSet中单条消息格式
    * |------- | ---- | ----- | ------|------------|-----------|----------|-----|------------|------ |
    * | offset | size | CRC32 | magic | attributes | timestamp | key size | key | value size | value |
    * |------- | ---- | ----- | ------|------------|-----------|----------|-----|------------|------ |
    * 这个方法只会更新offset，验证并更新CRC32、时间戳等信息
    * @param offsetCounter offset自增器
    * @param now 当前时间戳
    * @param compactedTopic 是否是压缩主题
    * @param timestampType 时间戳类型
    * @param timestampDiffMaxMs
    * @return
    */
  private def validateNonCompressedMessagesAndAssignOffsetInPlace(offsetCounter: LongRef,
                                                                  now: Long,
                                                                  compactedTopic: Boolean,
                                                                  timestampType: TimestampType,
                                                                  timestampDiffMaxMs: Long): ByteBufferMessageSet = {
    // do in-place validation and offset assignment
    var messagePosition = 0
    buffer.mark()
    while (messagePosition < sizeInBytes - MessageSet.LogOverhead) {
      buffer.position(messagePosition)
      // 写入offset
      buffer.putLong(offsetCounter.getAndIncrement())
      // 获取消息大小
      val messageSize = buffer.getInt()
      // 得到标识消息数据的Buffer切片
      val messageBuffer = buffer.slice()
      // 设置limit为当前遍历到的Message数据尾部
      messageBuffer.limit(messageSize)
      // 创建Message对象
      val message = new Message(messageBuffer)
      // 验证消息的键，如果是压缩主题，消息必须要有键
      validateMessageKey(message, compactedTopic)
      if (message.magic > Message.MagicValue_V0) {
        // 当魔数大于V0时，还需要验证消息的时间戳
        validateTimestamp(message, now, timestampType, timestampDiffMaxMs)
        if (timestampType == TimestampType.LOG_APPEND_TIME) { // 时间戳类型为追加类型
          // 写入当前时间戳
          message.buffer.putLong(Message.TimestampOffset, now)
          // 更新attributes中的时间戳类型标识位
          message.buffer.put(Message.AttributesOffset, timestampType.updateAttributes(message.attributes))
          // 写入CRC校验码
          Utils.writeUnsignedInt(message.buffer, Message.CrcOffset, message.computeChecksum)
        }
      }
      // 更新messagePosition
      messagePosition += MessageSet.LogOverhead + messageSize
    }
    buffer.reset()
    this
  }

  private def validateMessageKey(message: Message, compactedTopic: Boolean) {
    if (compactedTopic && !message.hasKey)
      throw new InvalidMessageException("Compacted topic cannot accept message without key.")
  }

  /**
   * This method validates the timestamps of a message.
   * If the message is using create time, this method checks if it is within acceptable range.
   */
  private def validateTimestamp(message: Message,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long) {
    // 时间戳类型为创建时间，消息时间戳与当前时间的间隔不可大于timestampDiffMaxMs
    if (timestampType == TimestampType.CREATE_TIME && math.abs(message.timestamp - now) > timestampDiffMaxMs)
      throw new InvalidTimestampException(s"Timestamp ${message.timestamp} of message is out of range. " +
        s"The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}")
    // 时间戳类型不可为追加时间
    if (message.timestampType == TimestampType.LOG_APPEND_TIME)
      throw new InvalidTimestampException(s"Invalid timestamp type in message $message. Producer should not set " +
        s"timestamp type to LogAppendTime.")
  }

  /**
   * The total number of bytes in this message set, including any partial trailing messages
   */
  def sizeInBytes: Int = buffer.limit

  /**
   * The total number of bytes in this message set not including any partial, trailing messages
   */
  def validBytes: Int = shallowValidBytes

  /**
   * Two message sets are equal if their respective byte buffers are equal
   */
  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def hashCode: Int = buffer.hashCode

}
