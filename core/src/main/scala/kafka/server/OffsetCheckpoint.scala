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
package kafka.server

import java.nio.file.{FileSystems, Paths}
import java.util.regex.Pattern

import org.apache.kafka.common.utils.Utils

import scala.collection._
import kafka.utils.Logging
import kafka.common._
import java.io._

object OffsetCheckpoint {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private val CurrentVersion = 0
}

/**
 * This class saves out a map of topic/partition=>offsets to a file
 */
class OffsetCheckpoint(val file: File) extends Logging {
  import OffsetCheckpoint._
  private val path = file.toPath.toAbsolutePath
  // 新的tmp文件
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()
  file.createNewFile() // in case the file doesn't exist

  /**
    * 最终存储的格式如下：
    * 0
    * 20
    * topic_test1 6 0
    * topic_test2 1 0
    * topic_test1 13 0
    * topic_test2 3 0
    * ...
    *
    * 第一行表示当前版本，第二行表示该LogDir目录下有多少个partition目录
    * 后续每行存储了三个信息：topic partition编号 recovery-checkpoint，有多少个partition目录就有多少行
    *
    * @param offsets
    */
  def write(offsets: Map[TopicAndPartition, Long]) {
    // 加锁
    lock synchronized {
      // write to temp file and then swap with the existing file
      // 将数据先写入tmp文件
      val fileOutputStream = new FileOutputStream(tempPath.toFile)
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream))
      try {
        // 写入版本号
        writer.write(CurrentVersion.toString)
        writer.newLine()

        // 写入记录条数
        writer.write(offsets.size.toString)
        writer.newLine()

        // 循环写入topic名称、分区编号以及对应Log的recoveryPoint
        offsets.foreach { case (topicPart, offset) =>
          writer.write(s"${topicPart.topic} ${topicPart.partition} $offset")
          writer.newLine()
        }

        writer.flush()
        // 将写入的数据刷新到磁盘
        fileOutputStream.getFD().sync()
      } catch {
        case e: FileNotFoundException =>
          if (FileSystems.getDefault.isReadOnly) {
            fatal("Halting writes to offset checkpoint file because the underlying file system is inaccessible : ", e)
            Runtime.getRuntime.halt(1)
          }
          throw e
      } finally {
        writer.close()
      }
      // 使用tmp文件替换原来的RecoveryPointCheckpoint文件
      Utils.atomicMoveWithFallback(tempPath, path)
    }
  }

  // 读取checkpoint文件
  def read(): Map[TopicAndPartition, Long] = {

    def malformedLineException(line: String) =
      new IOException(s"Malformed line in offset checkpoint file: $line'")

    // 加锁
    lock synchronized {
      // 读取器
      val reader = new BufferedReader(new FileReader(file))
      var line: String = null
      try {
        // 读取版本号
        line = reader.readLine()
        if (line == null)
          return Map.empty
        val version = line.toInt
        // 针对版本号进行不同的处理
        version match {
          // 当前版本号
          case CurrentVersion =>
            // 读取期望大小
            line = reader.readLine()
            if (line == null)
              return Map.empty
            val expectedSize = line.toInt
            val offsets = mutable.Map[TopicAndPartition, Long]()

            /**
              * 分别读取每行topic、partition、checkpoint offset信息
              * 并转换为Map[TopicAndPartition, Long]格式
              */
            line = reader.readLine()
            while (line != null) {
              WhiteSpacesPattern.split(line) match {
                case Array(topic, partition, offset) =>
                  offsets += TopicAndPartition(topic, partition.toInt) -> offset.toLong
                  line = reader.readLine()
                case _ => throw malformedLineException(line)
              }
            }
            // 检查大小是否匹配
            if (offsets.size != expectedSize)
              throw new IOException(s"Expected $expectedSize entries but found only ${offsets.size}")
            // 返回得到的Map[TopicAndPartition, Long]集合
            offsets
          case _ =>
            throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version)
        }
      } catch {
        case e: NumberFormatException => throw malformedLineException(line)
      } finally {
        reader.close()
      }
    }
  }
  
}
