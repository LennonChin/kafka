/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.record;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.AbstractIterator;

/**
 * A {@link Records} implementation backed by a ByteBuffer.
 */
public class MemoryRecords implements Records {

    private final static int WRITE_LIMIT_FOR_READABLE_ONLY = -1;

    // the compressor used for appends-only
	// 压缩器
    private final Compressor compressor;

    // the write limit for writable buffer, which may be smaller than the buffer capacity
	// 记录buffer最多可以写入的字节数
    private final int writeLimit;

    // the capacity of the initial buffer, which is only used for de-allocation of writable records
    private final int initialCapacity;

    // the underlying buffer used for read; while the records are still writable it is null
    // 保存消息数据的NIO ByteBuffer
    private ByteBuffer buffer;

    // indicate if the memory records is writable or not (i.e. used for appends or read-only)
	// 用于标识MemoryRecords是否可写，在MemoryRecord发送前会将该字段置为false只读
    private boolean writable;

    // Construct a writable memory records
    private MemoryRecords(ByteBuffer buffer, CompressionType type, boolean writable, int writeLimit) {
        this.writable = writable;
        this.writeLimit = writeLimit;
        this.initialCapacity = buffer.capacity();
        if (this.writable) {
            this.buffer = null;
            this.compressor = new Compressor(buffer, type);
        } else {
            this.buffer = buffer;
            this.compressor = null;
        }
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type, int writeLimit) {
        return new MemoryRecords(buffer, type, true, writeLimit);
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type) {
        // use the buffer capacity as the default write limit
        return emptyRecords(buffer, type, buffer.capacity());
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer, CompressionType.NONE, false, WRITE_LIMIT_FOR_READABLE_ONLY);
    }

    /**
     * Append the given record and offset to the buffer
     */
    public void append(long offset, Record record) {
    	// 判断是否可写
        if (!writable)
            throw new IllegalStateException("Memory records is not writable");

        // 使用压缩器将数据写入到buffer
        int size = record.size();
        compressor.putLong(offset);
        compressor.putInt(size);
        compressor.put(record.buffer());
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        record.buffer().rewind();
    }

    /**
     * Append a new record and offset to the buffer
     * @return crc of the record
     */
    public long append(long offset, long timestamp, byte[] key, byte[] value) {
        if (!writable)
            throw new IllegalStateException("Memory records is not writable");
        // 消息大小
        int size = Record.recordSize(key, value);
        // 向压缩器中添加offset记录
        compressor.putLong(offset);
        // 向压缩器中添加消息大小记录
        compressor.putInt(size);
        // 向压缩器中添加消息数据
        long crc = compressor.putRecord(timestamp, key, value);
        // 压缩器记录写入数据的大小
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        return crc;
    }

    /**
     * Check if we have room for a new record containing the given key/value pair
     *
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is really used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     *
     * There is an exceptional case when appending a single message whose size is larger than the batch size, the
     * capacity will be the message size which is larger than the write limit, i.e. the batch size. In this case
     * the checking should be based on the capacity of the initialized buffer rather than the write limit in order
     * to accept this single record.
	 * 根据Compressor估算的已写字节数，估计MemoryRecords剩余空间是否足够写入指定的数据。
     */
    public boolean hasRoomFor(byte[] key, byte[] value) {
    	// 检查是否可写，如果不可写直接返回false
        if (!this.writable)
            return false;
	
		/**
		 * this.compressor.numRecordsWritten()：已写入压缩器的记录数
		 * Records.LOG_OVERHEAD：包括SIZE_LENGTH和OFFSET_LENGTH，分别表示记录长度和偏移量
		 * this.initialCapacity：buffer的初始容量
		 * this.compressor.estimatedBytesWritten()：估算的压缩器已写入字节数
		 * this.writeLimit：buffer最多还可写入的字节数
		 *
		 * 判断逻辑如下：
		 * 1. 如果写入压缩器的记录数为0，则判断是否 buffer的初始容量 >= SIZE_LENGTH + OFFSET_LENGTH + 记录大小；
		 * 2. 否则判断是否 buffer最多还可写入的字节数 >= 估算的压缩器已写入字节数 + SIZE_LENGTH + OFFSET_LENGTH + 记录大小；
		 */
		return this.compressor.numRecordsWritten() == 0 ?
            this.initialCapacity >= Records.LOG_OVERHEAD + Record.recordSize(key, value) :
            this.writeLimit >= this.compressor.estimatedBytesWritten() + Records.LOG_OVERHEAD + Record.recordSize(key, value);
    }

    public boolean isFull() {
        return !this.writable || this.writeLimit <= this.compressor.estimatedBytesWritten();
    }

    /**
     * Close this batch for no more appends
     */
    public void close() {
        if (writable) {
            // close the compressor to fill-in wrapper message metadata if necessary
            compressor.close();

            // flip the underlying buffer to be ready for reads
            buffer = compressor.buffer();
            buffer.flip();

            // reset the writable flag
            writable = false;
        }
    }

    /**
     * The size of this record set
	 * 对于可写的MemoryRecords，返回的是ByteBufferOutputStream.buffer字段的大小；
	 * 对于只读MemoryRecords，返回的是MemoryRecords.buffer的大小。
     */
    public int sizeInBytes() {
        if (writable) {
            return compressor.buffer().position();
        } else {
            return buffer.limit();
        }
    }

    /**
     * The compression rate of this record set
     */
    public double compressionRate() {
        if (compressor == null)
            return 1.0;
        else
            return compressor.compressionRate();
    }

    /**
     * Return the capacity of the initial buffer, for writable records
     * it may be different from the current buffer's capacity
     */
    public int initialCapacity() {
        return this.initialCapacity;
    }

    /**
     * Get the byte buffer that backs this records instance for reading
     */
    public ByteBuffer buffer() {
        if (writable)
            throw new IllegalStateException("The memory records must not be writable any more before getting its underlying buffer");

        return buffer.duplicate();
    }

    @Override
    public Iterator<LogEntry> iterator() {
        if (writable) {
            // flip on a duplicate buffer for reading
            return new RecordsIterator((ByteBuffer) this.buffer.duplicate().flip(), false);
        } else {
            // do not need to flip for non-writable buffer
            return new RecordsIterator(this.buffer.duplicate(), false);
        }
    }
    
    @Override
    public String toString() {
        Iterator<LogEntry> iter = iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogEntry entry = iter.next();
            builder.append('(');
            builder.append("offset=");
            builder.append(entry.offset());
            builder.append(",");
            builder.append("record=");
            builder.append(entry.record());
            builder.append(")");
        }
        builder.append(']');
        return builder.toString();
    }

    /** Visible for testing */
    public boolean isWritable() {
        return writable;
    }

    // 消息记录迭代器
    public static class RecordsIterator extends AbstractIterator<LogEntry> {
        // 指向MemoryRecords的buffer字段
        private final ByteBuffer buffer;
        // 读取buffer的输入流，如果迭代压缩消息，则是对应的解压缩输入流。
        private final DataInputStream stream;
        // 压缩类型
        private final CompressionType type;
        // 是否是迭代压缩消息，为true时表示迭代非压缩消息，为false时表示迭代压缩消息
        private final boolean shallow;
        // 迭代压缩消息的内层迭代器
        private RecordsIterator innerIter;

        // The variables for inner iterator
        /**
         * 内层迭代器需要迭代的压缩消息集合，其中LogEntry中封装了消息及其offset。
         * 外层迭代器的此字段始终为null。
         */
        private final ArrayDeque<LogEntry> logEntries;
        /**
         * 在内层迭代器迭代压缩消息时使用，用于记录压缩消息中第一个消息的offset，并根据此字段计算每个消息的offset。
         * 外层迭代器的此字段始终为-1。
         */
        private final long absoluteBaseOffset;

        // 用于创建外层迭代器
        public RecordsIterator(ByteBuffer buffer, boolean shallow) {
            // 外层消息是非压缩的
            this.type = CompressionType.NONE;
            // 指向MemoryRecords的buffer字段
            this.buffer = buffer;
            // 标识是否为深层迭代器
            this.shallow = shallow;
            // 输入流
            this.stream = new DataInputStream(new ByteBufferInputStream(buffer));
            // 外层迭代器的logEntries恒为null，absoluteBaseOffset恒为-1
            this.logEntries = null;
            this.absoluteBaseOffset = -1;
        }

        // Private constructor for inner iterator.
        // 用于创建内层迭代器
        private RecordsIterator(LogEntry entry) {
            // 指定内层压缩消息的压缩类型
            this.type = entry.record().compressionType();
            this.buffer = entry.record().value();
            // 标识为深层压缩器
            this.shallow = true;
            // 创建指定压缩类型的输入流
            this.stream = Compressor.wrapForInput(new ByteBufferInputStream(this.buffer), type, entry.record().magic());
            // 外层消息的offset
            long wrapperRecordOffset = entry.offset();
            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset.
            if (entry.record().magic() > Record.MAGIC_VALUE_V0) {
                this.logEntries = new ArrayDeque<>();
                long wrapperRecordTimestamp = entry.record().timestamp();
                // 在while循环中将内层消息全部解压并添加到logEntries集合中
                while (true) {
                    try {
                        /**
                         * getNextEntryFromStream()方法对于内外层消息的作用并不一样
                         *  - 对于内层消息，该方法是读取并解压缩消息
                         *  - 对于外层消息，则仅仅是读取消息
                         */
                        LogEntry logEntry = getNextEntryFromStream();
                        // 根据LogEntry对象创建带有时间戳的Record记录对象
                        Record recordWithTimestamp = new Record(logEntry.record().buffer(),
                                                                wrapperRecordTimestamp,
                                                                entry.record().timestampType());
                        // 再次封装LogEntry并将其添加到logEntries
                        logEntries.add(new LogEntry(logEntry.offset(), recordWithTimestamp));
                    } catch (EOFException e) {
                        break;
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                }
                // 计算absoluteBaseOffset
                this.absoluteBaseOffset = wrapperRecordOffset - logEntries.getLast().offset();
            } else {
                this.logEntries = null;
                this.absoluteBaseOffset = -1;
            }

        }

        /*
         * Read the next record from the buffer.
         * 
         * Note that in the compressed message set, each message value size is set as the size of the un-compressed
         * version of the message value, so when we do de-compression allocating an array of the specified size for
         * reading compressed value data is sufficient.
         */
        @Override
        protected LogEntry makeNext() {
            // 内层迭代是否完成
            if (innerDone()) {
                try {
                    // 获取消息
                    LogEntry entry = getNextEntry();
                    // No more record to return.
                    // 获取不到消息，调用allDone()结束迭代
                    if (entry == null)
                        return allDone();

                    // Convert offset to absolute offset if needed.
                    // 在内层迭代器中计算每个消息的absoluteOffset
                    if (absoluteBaseOffset >= 0) {
                        long absoluteOffset = absoluteBaseOffset + entry.offset();
                        entry = new LogEntry(absoluteOffset, entry.record());
                    }

                    // decide whether to go shallow or deep iteration if it is compressed
                    // 根据压缩类型和shallow参数觉得是否创建内层迭代器
                    CompressionType compression = entry.record().compressionType();
                    if (compression == CompressionType.NONE || shallow) {
                        // 无压缩类型或者是浅层迭代，直接返回entry
                        return entry;
                    } else {
                        // init the inner iterator with the value payload of the message,
                        // which will de-compress the payload to a set of messages;
                        // since we assume nested compression is not allowed, the deep iterator
                        // would not try to further decompress underlying messages
                        // There will be at least one element in the inner iterator, so we don't
                        // need to call hasNext() here.
                        // 否则创建内层迭代器
                        innerIter = new RecordsIterator(entry);
                        // 返回内层迭代器的消息
                        return innerIter.next();
                    }
                } catch (EOFException e) {
                    return allDone();
                } catch (IOException e) {
                    throw new KafkaException(e);
                }
            } else {
                // 内层迭代还未完成，返回内层迭代的消息数据
                return innerIter.next();
            }
        }

        private LogEntry getNextEntry() throws IOException {
            if (logEntries != null)
                // 从logEntries队列中获取LogEntry
                return getNextEntryFromEntryList();
            else
                // 从buffer中获取LogEntry
                return getNextEntryFromStream();
        }

        private LogEntry getNextEntryFromEntryList() {
            // 从logEntries中获取LogEntry
            return logEntries.isEmpty() ? null : logEntries.remove();
        }

        // 读取消息
        private LogEntry getNextEntryFromStream() throws IOException {
            // read the offset
            // 读取消息offset
            long offset = stream.readLong();
            // read record size
            // 读取消息size
            int size = stream.readInt();
            // 如果消息size小于0，则抛出IllegalStateException异常
            if (size < 0)
                throw new IllegalStateException("Record with size " + size);
            // read the record, if compression is used we cannot depend on size
            // and hence has to do extra copy
            ByteBuffer rec;
            if (type == CompressionType.NONE) {
                // 非压缩消息
                /**
                 * 为buffer创建一个切片
                 * slice()方法会从buffer的position开始创建一个新的buffer
                 * 新buffer与旧buffer共享同样的数据
                 * 但新旧buffer的position、limit及mark是相互独立的
                 */
                rec = buffer.slice();
                // 计算新的position
                int newPos = buffer.position() + size;
                // 判断position是否合法
                if (newPos > buffer.limit())
                    return null;
                // 定位到新的position
                buffer.position(newPos);
                // 设置limit为size
                rec.limit(size);
            } else {
                // 压缩消息
                // 创建size大小的字节数组recordBuffer
                byte[] recordBuffer = new byte[size];
                // 通过流读取数据到字节数组recordBuffer
                stream.readFully(recordBuffer, 0, size);
                // 将字节数组recordBuffer转换为ByteBuffer
                rec = ByteBuffer.wrap(recordBuffer);
            }
            // 返回LogEntry对象
            return new LogEntry(offset, new Record(rec));
        }

        /**
         * 内层迭代是否结束
         * 当内层迭代器为null，或内层迭代器不为null但没有下一项元素了，即表示内层循环结束了
         */
        private boolean innerDone() {
            return innerIter == null || !innerIter.hasNext();
        }
    }
}
