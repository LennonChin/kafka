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
package org.apache.kafka.common.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

/**
 * A sensor applies a continuous sequence of numerical values to a set of associated metrics. For example a sensor on
 * message size would record a sequence of message sizes using the {@link #record(double)} api and would maintain a set
 * of metrics about request sizes such as the average or max.
 */
public final class Sensor {

    private final Metrics registry;
    // 当前Sensor对象的名称，Metrics通过该名称区别不同的Sensor对象
    private final String name;
    // Sensor是可以分为多层的，该字段指定了当前Sensor对象的父Sensor
    private final Sensor[] parents;
    // 保存构成Sensor的度量对象
    private final List<Stat> stats;
    /**
     * 保存构成Sensor的KafkaMetric对象，
     * 当度量对象添加进Sensor时会创建对应的KafkaMetric对象（CompoundStat会创建多个），
     * 并保存到此集合中
     */
    private final List<KafkaMetric> metrics;
    // 默认的配置信息
    private final MetricConfig config;
    private final Time time;
    // 最后一次执行record()方法的时间戳
    private volatile long lastRecordTime;
    /**
     * 长时间未使用Sensor会被认为是过期Sensor，
     * 由ExpireSensorTask线程负责进行清理，此字段记录成为过期Sensor”的阈值
     */
    private final long inactiveSensorExpirationTimeMs;

    Sensor(Metrics registry, String name, Sensor[] parents, MetricConfig config, Time time, long inactiveSensorExpirationTimeSeconds) {
        super();
        this.registry = registry;
        this.name = Utils.notNull(name);
        this.parents = parents == null ? new Sensor[0] : parents;
        this.metrics = new ArrayList<>();
        this.stats = new ArrayList<>();
        this.config = config;
        this.time = time;
        this.inactiveSensorExpirationTimeMs = TimeUnit.MILLISECONDS.convert(inactiveSensorExpirationTimeSeconds, TimeUnit.SECONDS);
        this.lastRecordTime = time.milliseconds();
        checkForest(new HashSet<Sensor>());
    }

    /* Validate that this sensor doesn't end up referencing itself */
    private void checkForest(Set<Sensor> sensors) {
        if (!sensors.add(this))
            throw new IllegalArgumentException("Circular dependency in sensors: " + name() + " is its own parent.");
        for (int i = 0; i < parents.length; i++)
            parents[i].checkForest(sensors);
    }

    /**
     * The name this sensor is registered with. This name will be unique among all registered sensors.
     */
    public String name() {
        return this.name;
    }

    /**
     * Record an occurrence, this is just short-hand for {@link #record(double) record(1.0)}
     */
    public void record() {
        record(1.0);
    }

    /**
     * Record a value with this sensor
     * @param value The value to record
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double value) {
        record(value, time.milliseconds());
    }

    /**
     * Record a value at a known time. This method is slightly faster than {@link #record(double)} since it will reuse
     * the time stamp.
     * @param value The value we are recording
     * @param timeMs The current POSIX time in milliseconds
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double value, long timeMs) {
        this.lastRecordTime = timeMs;
        synchronized (this) {
            // increment all the stats
            // 调用stats集合中每个stat对象的record()方法
            for (int i = 0; i < this.stats.size(); i++)
                this.stats.get(i).record(config, value, timeMs);
            // 检测是否超出了MetricConfig指定的上下西限
            checkQuotas(timeMs);
        }
        for (int i = 0; i < parents.length; i++)
            // 调用每个父Sensor的record()方法
            parents[i].record(value, timeMs);
    }

    /**
     * Check if we have violated our quota for any metric that has a configured quota
     * @param timeMs
     */
    private void checkQuotas(long timeMs) {
        for (int i = 0; i < this.metrics.size(); i++) {
            KafkaMetric metric = this.metrics.get(i);
            // 获取MetricConfig对象
            MetricConfig config = metric.config();
            if (config != null) {
                // 获取MetricsConfig对象中的Quota对象
                Quota quota = config.quota();
                if (quota != null) {
                    // 计算最终的度量值
                    double value = metric.value(timeMs);
                    // 检测度量值是否超出上下限
                    if (!quota.acceptable(value)) {
                        throw new QuotaViolationException(String.format(
                            "'%s' violated quota. Actual: %f, Threshold: %f",
                            metric.metricName(),
                            value,
                            quota.bound()));
                    }
                }
            }
        }
    }

    /**
     * Register a compound statistic with this sensor with no config override
     */
    public void add(CompoundStat stat) {
        add(stat, null);
    }

    /**
     * Register a compound statistic with this sensor which yields multiple measurable quantities (like a histogram)
     * @param stat The stat to register
     * @param config The configuration for this stat. If null then the stat will use the default configuration for this
     *        sensor.
     */
    public synchronized void add(CompoundStat stat, MetricConfig config) {
        this.stats.add(Utils.notNull(stat));
        // 遍历CompoundStat中每个子Stat对象
        for (NamedMeasurable m : stat.stats()) {
            // 创建KafkaMetric对象
            KafkaMetric metric = new KafkaMetric(this, m.name(), m.stat(), config == null ? this.config : config, time);
            // 将KafkaMetric保存到Metrics中，创建并注册对应的MBean
            this.registry.registerMetric(metric);
            // 添加到metrics集合
            this.metrics.add(metric);
        }
    }

    /**
     * Register a metric with this sensor
     * @param metricName The name of the metric
     * @param stat The statistic to keep
     */
    public void add(MetricName metricName, MeasurableStat stat) {
        add(metricName, stat, null);
    }

    /**
     * Register a metric with this sensor
     * 向Sensor中添加Stat
     *
     * @param metricName The name of the metric
     * @param stat The statistic to keep
     * @param config A special configuration for this metric. If null use the sensor default configuration.
     */
    public synchronized void add(MetricName metricName, MeasurableStat stat, MetricConfig config) {
        // 创建KafkaMetric对象
        KafkaMetric metric = new KafkaMetric(new Object(),
                                             Utils.notNull(metricName),
                                             Utils.notNull(stat),
                                             config == null ? this.config : config,
                                             time);
        // 将KafkaMetric保存到Metrics中，创建并注册对应的MBean
        this.registry.registerMetric(metric);
        // 添加到metrics集合
        this.metrics.add(metric);
        // 添加到stats集合
        this.stats.add(stat);
    }

    /**
     * Return true if the Sensor is eligible for removal due to inactivity.
     *        false otherwise
     */
    public boolean hasExpired() {
        return (time.milliseconds() - this.lastRecordTime) > this.inactiveSensorExpirationTimeMs;
    }

    synchronized List<KafkaMetric> metrics() {
        return Collections.unmodifiableList(this.metrics);
    }
}
