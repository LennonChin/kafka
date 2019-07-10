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
package org.apache.kafka.common.metrics.stats;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * A SampledStat records a single scalar value measured over one or more samples. Each sample is recorded over a
 * configurable window. The window can be defined by number of events or elapsed time (or both, if both are given the
 * window is complete when <i>either</i> the event count or elapsed time criterion is met).
 * <p>
 * All the samples are combined to produce the measurement. When a window is complete the oldest sample is cleared and
 * recycled to begin recording the next sample.
 * 
 * Subclasses of this class define different statistics measured using this basic pattern.
 *
 * 表示一个抽样的度量值，除了Total外的其他MeasurableStat接口实现都依赖它的功能
 */
public abstract class SampledStat implements MeasurableStat {
    // 指定每个样本的初始值
    private double initialValue;
    // 当前使用的Sample的下
    private int current = 0;
    // List类型，保存当前SampledStat中的多个Sample
    protected List<Sample> samples;

    public SampledStat(double initialValue) {
        this.initialValue = initialValue;
        this.samples = new ArrayList<Sample>(2);
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        // 得到当前的Sample对象
        Sample sample = current(timeMs);
        // 检测当前Sample是否已经完成取样
        if (sample.isComplete(timeMs, config))
            // 获取下一个Sample
            sample = advance(config, timeMs);
        // 更新Sample，该方法是抽象方法
        update(sample, config, value, timeMs);
        // 增加事件数
        sample.eventCount += 1;
    }

    // 根据配置指定的Sample数量决定创建新Sample还是使用之前的Sample对象
    private Sample advance(MetricConfig config, long timeMs) {
        this.current = (this.current + 1) % config.samples();
        if (this.current >= samples.size()) {
            // 创建新的Sample对象
            Sample sample = newSample(timeMs);
            this.samples.add(sample);
            return sample;
        } else {
            Sample sample = current(timeMs);
            // 重用之前的Sample对象，一共就2个
            sample.reset(timeMs);
            return sample;
        }
    }

    protected Sample newSample(long timeMs) {
        return new Sample(this.initialValue, timeMs);
    }

    // 会将过期的Sample重置，调用combine()方法完成计算，该方法是抽象方法
    @Override
    public double measure(MetricConfig config, long now) {
        purgeObsoleteSamples(config, now);
        return combine(this.samples, config, now);
    }

    public Sample current(long timeMs) {
        if (samples.size() == 0)
            this.samples.add(newSample(timeMs));
        return this.samples.get(this.current);
    }

    public Sample oldest(long now) {
        if (samples.size() == 0)
            this.samples.add(newSample(now));
        Sample oldest = this.samples.get(0);
        for (int i = 1; i < this.samples.size(); i++) {
            Sample curr = this.samples.get(i);
            if (curr.lastWindowMs < oldest.lastWindowMs)
                oldest = curr;
        }
        return oldest;
    }

    protected abstract void update(Sample sample, MetricConfig config, double value, long timeMs);

    public abstract double combine(List<Sample> samples, MetricConfig config, long now);

    /* Timeout any windows that have expired in the absence of any events */
    protected void purgeObsoleteSamples(MetricConfig config, long now) {
        // 计算过期时长
        long expireAge = config.samples() * config.timeWindowMs();
        for (int i = 0; i < samples.size(); i++) {
            Sample sample = this.samples.get(i);
            if (now - sample.lastWindowMs >= expireAge)
                // 检测到Sample过期，将其重置
                sample.reset(now);
        }
    }

    protected static class Sample {
        // 指定样本的初始值
        public double initialValue;
        // 记录当前样本的事件数
        public long eventCount;
        // 记录当前样本的时间窗口开始的时间戳
        public long lastWindowMs;
        // 记录样本的值
        public double value;

        public Sample(double initialValue, long now) {
            this.initialValue = initialValue;
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
        }

        public void reset(long now) {
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
        }

        // 检测eventCount和lastWindows决定当前样本是否已经取样完成
        public boolean isComplete(long timeMs, MetricConfig config) {
            return timeMs - lastWindowMs >= config.timeWindowMs() || // 检测时间窗口
                    eventCount >= config.eventWindow(); // 检测事件数
        }
    }

}
