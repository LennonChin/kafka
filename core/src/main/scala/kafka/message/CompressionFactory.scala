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

import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.GZIPInputStream
import java.io.InputStream

import org.apache.kafka.common.record.{KafkaLZ4BlockInputStream, KafkaLZ4BlockOutputStream}

object CompressionFactory {

  /**
    * 根据压缩器、消息版本（魔数版本）、输出流创建经过压缩器包装后的输出流，进行压缩
    * @param compressionCodec 压缩器
    * @param messageVersion 消息版本
    * @param stream 输出流
    * @return
    */
  def apply(compressionCodec: CompressionCodec, messageVersion: Byte, stream: OutputStream): OutputStream = {
    compressionCodec match {
      case DefaultCompressionCodec => new GZIPOutputStream(stream)
      case GZIPCompressionCodec => new GZIPOutputStream(stream)
      case SnappyCompressionCodec => 
        import org.xerial.snappy.SnappyOutputStream
        new SnappyOutputStream(stream)
      case LZ4CompressionCodec =>
        new KafkaLZ4BlockOutputStream(stream, messageVersion == Message.MagicValue_V0)
      case _ =>
        throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
    }
  }

  /**
    * 根据压缩器、消息版本（魔数版本）、输入流创建经过压缩器包装后的输入流，进行解压
    * @param compressionCodec 压缩器
    * @param messageVersion 消息版本
    * @param stream 输入流
    * @return
    */
  def apply(compressionCodec: CompressionCodec, messageVersion: Byte, stream: InputStream): InputStream = {
    compressionCodec match {
      case DefaultCompressionCodec => new GZIPInputStream(stream)
      case GZIPCompressionCodec => new GZIPInputStream(stream)
      case SnappyCompressionCodec => 
        import org.xerial.snappy.SnappyInputStream
        new SnappyInputStream(stream)
      case LZ4CompressionCodec =>
        new KafkaLZ4BlockInputStream(stream, messageVersion == Message.MagicValue_V0)
      case _ =>
        throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
    }
  }
}
