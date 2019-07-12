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
package org.apache.kafka.tools;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class ProducerPerformance {

    public static void main(String[] args) throws Exception {
        // 命令行参数解析
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /**
             * parse args
             * 解析命令行参数
             **/
            String topicName = res.getString("topic");
            long numRecords = res.getLong("numRecords");
            int recordSize = res.getInt("recordSize");
            int throughput = res.getInt("throughput");
            List<String> producerProps = res.getList("producerConfig");

            Properties props = new Properties();
            if (producerProps != null)
                for (String prop : producerProps) {
                    String[] pieces = prop.split("=");
                    if (pieces.length != 2)
                        throw new IllegalArgumentException("Invalid property: " + prop);
                    props.put(pieces[0], pieces[1]);
                }

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

            // 创建KafkaProducer
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

            /* setup perf test */
            // 根据--record-size参数创建测试消息的载荷
            byte[] payload = new byte[recordSize];
            Random random = new Random(0);
            for (int i = 0; i < payload.length; ++i)
                // 生产随机字节填充消息载荷
                payload[i] = (byte) (random.nextInt(26) + 65);
            // 创建ProducerRecord，主题名称由--topic参数指定
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, payload);

            // 创建Stats对象，用于指标的统计，其中--num-records参数指定了产生的消息个数
            Stats stats = new Stats(numRecords, 5000);

            // 记录开始时间
            long startMs = System.currentTimeMillis();

            // 创建限流器
            ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);

            // 循环发送消息
            for (int i = 0; i < numRecords; i++) {
                // 记录发送时间
                long sendStartMs = System.currentTimeMillis();
                Callback cb = stats.nextCompletion(sendStartMs, payload.length, stats);
                // 发送消息
                producer.send(record, cb);

                // 限流器限流
                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }
            }

            /* print final results */
            producer.close();

            // 打印统计信息
            stats.printTotal();
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }

    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--num-records")
                .action(store())
                .required(true)
                .type(Long.class)
                .metavar("NUM-RECORDS")
                .dest("numRecords")
                .help("number of messages to produce");

        parser.addArgument("--record-size")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("RECORD-SIZE")
                .dest("recordSize")
                .help("message size in bytes");

        parser.addArgument("--throughput")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec");

        parser.addArgument("--producer-props")
                 .nargs("+")
                 .required(true)
                 .metavar("PROP-NAME=PROP-VALUE")
                 .type(String.class)
                 .dest("producerConfig")
                 .help("kafka producer related configuaration properties like bootstrap.servers,client.id etc..");

        return parser;
    }

    private static class Stats {
        // 记录开始测试的时间戳
        private long start;
        // 当前时间窗口的起始时间戳
        private long windowStart;
        // 记录每个样本中的延迟
        private int[] latencies;
        // 样本个数，与指定发送的消息数量有关，默认是500000为一个样本
        private int sampling;
        // 记录迭代次数
        private int iteration;
        private int index;
        // 记录整个过程发送的消息个数
        private long count;
        // 记录发送消息的总字节数
        private long bytes;
        // 记录从消息发出到对应响应返回之间的延迟的最大值
        private int maxLatency;
        // 记录延迟的总时间
        private long totalLatency;
        // 当前时间窗口中发送消息的个数
        private long windowCount;
        // 记录当前时间窗口中最大的延时
        private int windowMaxLatency;
        // 记录当前时间窗口中延时的总时长
        private long windowTotalLatency;
        // 记录当前窗口发送的总字节数
        private long windowBytes;
        // 保存两次输出之间的时间间隔
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.index = 0;
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int iter, int latency, int bytes, long time) {
            // 计算发送消息个数
            this.count++;
            // 计算发送总字节数
            this.bytes += bytes;
            // 计算总延迟
            this.totalLatency += latency;
            // 计算最大延迟
            this.maxLatency = Math.max(this.maxLatency, latency);
            // 计算当前窗口发送消息个数
            this.windowCount++;
            // 计算当前窗口发送总字节数
            this.windowBytes += bytes;
            // 计算当前窗口的总延迟
            this.windowTotalLatency += latency;
            // 记录当前窗口的最大延迟
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);

            // 选择样本，更新latencies中对应的值
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            // 检测是否需要结束当前窗口，并开启新窗口
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
                              windowCount,
                              recsPerSec,
                              mbPerSec,
                              windowTotalLatency / (double) windowCount,
                              (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                              count,
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) count,
                              (double) maxLatency,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            // 计算消息的延迟
            int latency = (int) (now - start);
            // 使用Stat的record()方法进行记录
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
    }

}
