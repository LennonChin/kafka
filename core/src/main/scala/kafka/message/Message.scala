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

import java.nio._

import org.apache.kafka.common.record.TimestampType

import scala.math._
import kafka.utils._
import org.apache.kafka.common.utils.Utils

/**
 * Constants related to messages
 */
object Message {

  /**
   * The current offset and size for all the fixed-length fields
   */
  // CRC偏移量
  val CrcOffset = 0
  // CRC长度，4字节
  val CrcLength = 4
  // 魔数偏移量
  val MagicOffset = CrcOffset + CrcLength
  // 魔数长度，1字节
  val MagicLength = 1
  // attributes偏移量
  val AttributesOffset = MagicOffset + MagicLength
  // attributes长度，1字节
  val AttributesLength = 1
  // Only message format version 1 has the timestamp field.
  // 时间戳偏移量
  val TimestampOffset = AttributesOffset + AttributesLength
  // 时间戳长度，8字节
  val TimestampLength = 8
  // V0版本key size的偏移量（V0版本没有时间戳数据）
  val KeySizeOffset_V0 = AttributesOffset + AttributesLength
  // V1版本key size的偏移量（V1版本有时间戳数据）
  val KeySizeOffset_V1 = TimestampOffset + TimestampLength
  // key size的长度，4字节
  val KeySizeLength = 4
  // V0版本Key偏移量
  val KeyOffset_V0 = KeySizeOffset_V0 + KeySizeLength
  // V1版本Key偏移量
  val KeyOffset_V1 = KeySizeOffset_V1 + KeySizeLength
  // value size的长度，4字节
  val ValueSizeLength = 4

  // 消息的头信息长度
  private val MessageHeaderSizeMap = Map (
    // V0版本，没有时间戳
    (0: Byte) -> (CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength),
    // V1版本，有时间戳
    (1: Byte) -> (CrcLength + MagicLength + AttributesLength + TimestampLength + KeySizeLength + ValueSizeLength))

  /**
   * The amount of overhead bytes in a message
   * This value is only used to check if the message size is valid or not. So the minimum possible message bytes is
   * used here, which comes from a message in message format V0 with empty key and value.
    * 消息的overhead的大小，不包含键和值数据的长度
   */
  val MinMessageOverhead = KeyOffset_V0 + ValueSizeLength
  
  /**
   * The "magic" value
   * When magic value is 0, the message uses absolute offset and does not have a timestamp field.
   * When magic value is 1, the message uses relative offset and has a timestamp field.
    * 魔数，V0版本为0，V1版本为1
   */
  val MagicValue_V0: Byte = 0
  val MagicValue_V1: Byte = 1
  // 当前魔数为1
  val CurrentMagicValue: Byte = 1

  /**
   * Specifies the mask for the compression code. 3 bits to hold the compression codec.
   * 0 is reserved to indicate no compression
    * 取压缩器值的掩码
   */
  val CompressionCodeMask: Int = 0x07 // 00000000 00000000 00000000 00000111
  /**
   * Specifies the mask for timestamp type. 1 bit at the 4th least significant bit.
   * 0 for CreateTime, 1 for LogAppendTime
    * 取时间戳类型的掩码
   */
  val TimestampTypeMask: Byte = 0x08 // 00000000 00000000 00000000 00001000
  val TimestampTypeAttributeBitOffset: Int = 3 // 时间戳类型在attributes数据中的偏移量

  /**
   * Compression code for uncompressed messages
    * 表示无压缩器
   */
  val NoCompression: Int = 0

  /**
   * To indicate timestamp is not defined so "magic" value 0 will be used.
    * 表示无时间戳数据
   */
  val NoTimestamp: Long = -1

  /**
   * Give the header size difference between different message versions.
    * 比较两个魔数版本的消息的头信息的长度差
   */
  def headerSizeDiff(fromMagicValue: Byte, toMagicValue: Byte) : Int =
    MessageHeaderSizeMap(toMagicValue) - MessageHeaderSizeMap(fromMagicValue)


}

/**
 * A message. The format of an N byte message is the following:
 *
 * 1. 4 byte CRC32 of the message
 * 2. 1 byte "magic" identifier to allow format changes, value is 0 or 1
 * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version
 *    bit 0 ~ 2 : Compression codec.
 *      0 : no compression
 *      1 : gzip
 *      2 : snappy
 *      3 : lz4
 *    bit 3 : Timestamp type
 *      0 : create time
 *      1 : log append time
 *    bit 4 ~ 7 : reserved
 * 4. (Optional) 8 byte timestamp only if "magic" identifier is greater than 0
 * 5. 4 byte key length, containing length K
 * 6. K byte key
 * 7. 4 byte payload length, containing length V
 * 8. V byte payload
 *
 * Default constructor wraps an existing ByteBuffer with the Message object with no change to the contents.
 * @param buffer the byte buffer of this message.
 * @param wrapperMessageTimestamp the wrapper message timestamp, which is only defined when the message is an inner
 *                                message of a compressed message.
 * @param wrapperMessageTimestampType the wrapper message timestamp type, which is only defined when the message is an
 *                                    inner message of a compressed message.
 */
class Message(val buffer: ByteBuffer,
              private val wrapperMessageTimestamp: Option[Long] = None,
              private val wrapperMessageTimestampType: Option[TimestampType] = None) {
  
  import kafka.message.Message._

  /**
   * A constructor to create a Message
   * @param bytes The payload of the message 值数据载荷
   * @param key The key of the message (null, if none) 键
   * @param timestamp The timestamp of the message. 时间戳
   * @param timestampType The timestamp type of the message. 时间戳类型
   * @param codec The compression codec used on the contents of the message (if any) 压缩器
   * @param payloadOffset The offset into the payload array used to extract payload 读取bytes时的起始offset
   * @param payloadSize The size of the payload to use 读取bytes的字节数，payloadOffset和payloadSize用于控制量化读取bytes数组
   * @param magicValue the magic value to use 魔数
   */
  def this(bytes: Array[Byte],
           key: Array[Byte],
           timestamp: Long,
           timestampType: TimestampType,
           codec: CompressionCodec,
           payloadOffset: Int,
           payloadSize: Int,
           magicValue: Byte) = {
    this(ByteBuffer.allocate(Message.CrcLength +
                             Message.MagicLength +
                             Message.AttributesLength +
                              // 魔数为V0（值为0）时没有时间戳，为V1（值为1）时有时间戳
                             (if (magicValue == Message.MagicValue_V0) 0
                              else Message.TimestampLength) +
                             Message.KeySizeLength +
                              // 键长度，键为空时，键长度为0
                             (if(key == null) 0 else key.length) +
                             Message.ValueSizeLength +
                              // 值长度，bytes为空时，值长度为0
                              // bytes不为空时，如果payloadSize大于等于0则取payloadSize
                              // bytes不为空时，如果payloadSize小于0则计算bytes.length - payloadOffset
                             (if(bytes == null) 0
                              else if(payloadSize >= 0) payloadSize
                              else bytes.length - payloadOffset)))
    // 验证时间戳和魔数，这里的验证会根据魔数版本来判断时间戳是否合法
    validateTimestampAndMagicValue(timestamp, magicValue)
    // skip crc, we will fill that in at the end
    // 先跳过CRC，CRC部分会在最后填充
    buffer.position(MagicOffset)
    // 填入魔数
    buffer.put(magicValue)
    val attributes: Byte =
      if (codec.codec > 0)
        // 有压缩器，需要写入压缩器类型及时间戳类型
        timestampType.updateAttributes((CompressionCodeMask & codec.codec).toByte)
      // 无压缩器，attribute为0
      else 0
    // 填入attributes
    buffer.put(attributes)
    // Only put timestamp when "magic" value is greater than 0
    if (magic > MagicValue_V0)
      // 魔数大于V0时，填入时间戳
      buffer.putLong(timestamp)
    if(key == null) { // 键为空
      // 键长度，键为空时填入-1
      buffer.putInt(-1)
    } else { // 键不为空
      // 键长度，键不为空时填入键的长度
      buffer.putInt(key.length)
      // 填入键
      buffer.put(key, 0, key.length)
    }
    // 计算值长度
    val size = if(bytes == null) -1
               else if(payloadSize >= 0) payloadSize
               else bytes.length - payloadOffset
    // 填入值长度
    buffer.putInt(size)
    if(bytes != null)
      // 值不为空，填入值
      buffer.put(bytes, payloadOffset, size)
    // rewind，position移到0，准备填入CRC
    buffer.rewind()

    // now compute the checksum and fill it in
    // 计算并填入CRC
    Utils.writeUnsignedInt(buffer, CrcOffset, computeChecksum)
  }
  
  def this(bytes: Array[Byte], key: Array[Byte], timestamp: Long, codec: CompressionCodec, magicValue: Byte) =
    this(bytes = bytes, key = key, timestamp = timestamp, timestampType = TimestampType.CREATE_TIME, codec = codec, payloadOffset = 0, payloadSize = -1, magicValue = magicValue)
  
  def this(bytes: Array[Byte], timestamp: Long, codec: CompressionCodec, magicValue: Byte) =
    this(bytes = bytes, key = null, timestamp = timestamp, codec = codec, magicValue = magicValue)
  
  def this(bytes: Array[Byte], key: Array[Byte], timestamp: Long, magicValue: Byte) =
    this(bytes = bytes, key = key, timestamp = timestamp, codec = NoCompressionCodec, magicValue = magicValue)
    
  def this(bytes: Array[Byte], timestamp: Long, magicValue: Byte) =
    this(bytes = bytes, key = null, timestamp = timestamp, codec = NoCompressionCodec, magicValue = magicValue)

  def this(bytes: Array[Byte]) =
    this(bytes = bytes, key = null, timestamp = Message.NoTimestamp, codec = NoCompressionCodec, magicValue = Message.CurrentMagicValue)
    
  /**
   * Compute the checksum of the message from the message contents
    * 根据ByteBuffer数据计算checksum值
   */
  def computeChecksum: Long =
    CoreUtils.crc32(buffer.array, buffer.arrayOffset + MagicOffset,  buffer.limit - MagicOffset)
  
  /**
   * Retrieve the previously computed CRC for this message
    * 读取消息中的CRC值作为checksum
   */
  def checksum: Long = Utils.readUnsignedInt(buffer, CrcOffset)
  
    /**
   * Returns true if the crc stored with the message matches the crc computed off the message contents
      * 判断消息是否有效，即将消息中的checksum与计算出的checksum进行比较
   */
  def isValid: Boolean = checksum == computeChecksum
  
  /**
   * Throw an InvalidMessageException if isValid is false for this message
    * 保证消息是有效的，如果无效则抛出InvalidMessageException异常
   */
  def ensureValid() {
    if(!isValid)
      throw new InvalidMessageException(s"Message is corrupt (stored crc = ${checksum}, computed crc = ${computeChecksum})")
  }
  
  /**
   * The complete serialized size of this message in bytes (including crc, header attributes, etc)
    * 消息大小
   */
  def size: Int = buffer.limit

  /**
   * The position where the key size is stored.
    * 表示键大小的数据的偏移量
   */
  private def keySizeOffset = {
    // 当魔数为MagicValue_V0为KeySizeOffset_V0，否则为KeySizeOffset_V1
    if (magic == MagicValue_V0) KeySizeOffset_V0
    else KeySizeOffset_V1
  }

  /**
   * The length of the key in bytes
    * 键大小
   */
  def keySize: Int = buffer.getInt(keySizeOffset)
  
  /**
   * Does the message have a key?
    * 消息是否有键
   */
  def hasKey: Boolean = keySize >= 0
  
  /**
   * The position where the payload size is stored
    * 表示值大小的数据的偏移量
   */
  private def payloadSizeOffset = {
    // payLoadSize数据紧跟着键的数据，魔数不同时，键的偏移量不同
    if (magic == MagicValue_V0) KeyOffset_V0 + max(0, keySize)
    else KeyOffset_V1 + max(0, keySize)
  }
  
  /**
   * The length of the message value in bytes
    * 获取值数据的大小
   */
  def payloadSize: Int = buffer.getInt(payloadSizeOffset)
  
  /**
   * Is the payload of this message null
    * 判断值数据是否为空
   */
  def isNull: Boolean = payloadSize < 0
  
  /**
   * The magic version of this message
    * 获取魔数，偏移量是MagicOffset
   */
  def magic: Byte = buffer.get(MagicOffset)
  
  /**
   * The attributes stored with this message
    * 获取消息属性，一个字节长，偏移量是AttributesOffset
   */
  def attributes: Byte = buffer.get(AttributesOffset)

  /**
   * The timestamp of the message, only available when the "magic" value is greater than 0
   * When magic > 0, The timestamp of a message is determined in the following way:
   * 1. wrapperMessageTimestampType = None and wrapperMessageTimestamp is None - Uncompressed message, timestamp and timestamp type are in the message.
   * 2. wrapperMessageTimestampType = LogAppendTime and wrapperMessageTimestamp is defined - Compressed message using LogAppendTime
   * 3. wrapperMessageTimestampType = CreateTime and wrapperMessageTimestamp is defined - Compressed message using CreateTime
    * 获取时间戳，其含义由attribute的第3位确定，0表示创建时间，1表示追加时间；
    * magic值不同，消息的长度是不同的：
    *   - 当magic为0时，消息的offset使用绝对offset且消息格式中没有timestamp部分；
    *   - 当magic为1时，消息的offset使用相对offset且消息格式中存在timestamp部分。
   */
  def timestamp: Long = {
    if (magic == MagicValue_V0)
      Message.NoTimestamp
    // Case 2
    else if (wrapperMessageTimestampType.exists(_ == TimestampType.LOG_APPEND_TIME) && wrapperMessageTimestamp.isDefined)
      wrapperMessageTimestamp.get
    else // case 1, 3
      buffer.getLong(Message.TimestampOffset)
  }

  /**
   * The timestamp type of the message
    * 时间戳类型
   */
  def timestampType = {
    if (magic == MagicValue_V0)
      // 无时间戳
      TimestampType.NO_TIMESTAMP_TYPE
    else
      // 根据消息属性attributes来获取时间戳类型
      wrapperMessageTimestampType.getOrElse(TimestampType.forAttributes(attributes))
  }
  
  /**
   * The compression codec used with this message
    * 获取压缩器类型
   */
  def compressionCodec: CompressionCodec = 
    CompressionCodec.getCompressionCodec(buffer.get(AttributesOffset) & CompressionCodeMask)
  
  /**
   * A ByteBuffer containing the content of the message
    * 获取值数据
   */
  def payload: ByteBuffer = sliceDelimited(payloadSizeOffset)
  
  /**
   * A ByteBuffer containing the message key
    * 获取键数据
   */
  def key: ByteBuffer = sliceDelimited(keySizeOffset)

  /**
   * convert the message to specified format
    * 根据指定的魔数将消息转换为特定格式
   */
  def toFormatVersion(toMagicValue: Byte): Message = {
    if (magic == toMagicValue)
      // 指定魔数与当前魔数相同，直接返回即可
      this
    else {
      // 否则进行转换
      // 先创建需要的ByteBuffer
      val byteBuffer = ByteBuffer.allocate(size + Message.headerSizeDiff(magic, toMagicValue))
      // Copy bytes from old messages to new message
      // 使用convertToBuffer()方法进行数据拷贝
      convertToBuffer(toMagicValue, byteBuffer, Message.NoTimestamp)
      // 创建新的Message对象并返回
      new Message(byteBuffer)
    }
  }

  def convertToBuffer(toMagicValue: Byte,
                      byteBuffer: ByteBuffer,
                      now: Long,
                      timestampType: TimestampType = wrapperMessageTimestampType.getOrElse(TimestampType.forAttributes(attributes))) {
    if (byteBuffer.remaining() < size + headerSizeDiff(magic, toMagicValue))
      throw new IndexOutOfBoundsException("The byte buffer does not have enough capacity to hold new message format " +
        s"version $toMagicValue")
    if (toMagicValue == Message.MagicValue_V1) {
      // Up-conversion, reserve CRC and update magic byte
      byteBuffer.position(Message.MagicOffset)
      byteBuffer.put(Message.MagicValue_V1)
      byteBuffer.put(timestampType.updateAttributes(attributes))
      // Up-conversion, insert the timestamp field
      if (timestampType == TimestampType.LOG_APPEND_TIME)
        byteBuffer.putLong(now)
      else
        byteBuffer.putLong(Message.NoTimestamp)
      byteBuffer.put(buffer.array(), buffer.arrayOffset() + Message.KeySizeOffset_V0, size - Message.KeySizeOffset_V0)
    } else {
      // Down-conversion, reserve CRC and update magic byte
      byteBuffer.position(Message.MagicOffset)
      byteBuffer.put(Message.MagicValue_V0)
      byteBuffer.put(TimestampType.CREATE_TIME.updateAttributes(attributes))
      // Down-conversion, skip the timestamp field
      byteBuffer.put(buffer.array(), buffer.arrayOffset() + Message.KeySizeOffset_V1, size - Message.KeySizeOffset_V1)
    }
    // update crc value
    val newMessage = new Message(byteBuffer)
    Utils.writeUnsignedInt(byteBuffer, Message.CrcOffset, newMessage.computeChecksum)
    byteBuffer.rewind()
  }

  /**
   * Read a size-delimited byte buffer starting at the given offset
   */
  private def sliceDelimited(start: Int): ByteBuffer = {
    val size = buffer.getInt(start)
    if(size < 0) {
      null
    } else {
      var b = buffer.duplicate()
      b.position(start + 4)
      b = b.slice()
      b.limit(size)
      b.rewind
      b
    }
  }

  /**
   * Validate the timestamp and "magic" value
   */
  private def validateTimestampAndMagicValue(timestamp: Long, magic: Byte) {
    // 魔数必须是V0或V1版本中的一个
    if (magic != MagicValue_V0 && magic != MagicValue_V1)
      throw new IllegalArgumentException(s"Invalid magic value $magic")
    // 时间戳若小于0，则必须是-1
    if (timestamp < 0 && timestamp != NoTimestamp)
      throw new IllegalArgumentException(s"Invalid message timestamp $timestamp")
    // V0版本的魔数，时间戳必须是-1
    if (magic == MagicValue_V0 && timestamp != NoTimestamp)
      throw new IllegalArgumentException(s"Invalid timestamp $timestamp. Timestamp must be ${NoTimestamp} when magic = ${MagicValue_V0}")
  }

  override def toString(): String = {
    if (magic == MagicValue_V0)
      s"Message(magic = $magic, attributes = $attributes, crc = $checksum, key = $key, payload = $payload)"
    else
      s"Message(magic = $magic, attributes = $attributes, $timestampType = $timestamp, crc = $checksum, key = $key, payload = $payload)"
  }

  override def equals(any: Any): Boolean = {
    any match {
      case that: Message => this.buffer.equals(that.buffer)
      case _ => false
    }
  }
  
  override def hashCode(): Int = buffer.hashCode

}
