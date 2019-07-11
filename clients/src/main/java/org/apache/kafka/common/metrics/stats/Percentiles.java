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

import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.stats.Histogram.BinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.ConstantBinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.LinearBinScheme;

/**
 * A compound stat that reports one or more percentiles
 * 用于记录一个或多个百分比数的复合统计信息
 */
public class Percentiles extends SampledStat implements CompoundStat {

    // 桶大小：常量桶还是线性桶
    public static enum BucketSizing {
        CONSTANT, LINEAR
    }

    // 桶的数量
    private final int buckets;
    // Percentile数组，用于记录百分比值
    private final Percentile[] percentiles;
    private final BinScheme binScheme;

    public Percentiles(int sizeInBytes, double max, BucketSizing bucketing, Percentile... percentiles) {
        this(sizeInBytes, 0.0, max, bucketing, percentiles);
    }

    public Percentiles(int sizeInBytes, double min, double max, BucketSizing bucketing, Percentile... percentiles) {
        // 初始值是0
        super(0.0);
        // 记录传入的百分比值
        this.percentiles = percentiles;

        // 桶的数量
        this.buckets = sizeInBytes / 4;

        if (bucketing == BucketSizing.CONSTANT) {
            // 常量桶，构建ConstantBinScheme
            this.binScheme = new ConstantBinScheme(buckets, min, max);
        } else if (bucketing == BucketSizing.LINEAR) {
            // 线性桶，初始值需要不为0
            if (min != 0.0d)
                throw new IllegalArgumentException("Linear bucket sizing requires min to be 0.0.");
            // 构建LinearBinScheme
            this.binScheme = new LinearBinScheme(buckets, max);
        } else {
            throw new IllegalArgumentException("Unknown bucket type: " + bucketing);
        }
    }

    @Override
    public List<NamedMeasurable> stats() {
        List<NamedMeasurable> ms = new ArrayList<NamedMeasurable>(this.percentiles.length);
        for (Percentile percentile : this.percentiles) {
            final double pct = percentile.percentile();
            ms.add(new NamedMeasurable(percentile.name(), new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return value(config, now, pct / 100.0);
                }
            }));
        }
        return ms;
    }

    public double value(MetricConfig config, long now, double quantile) {
        // 清理过期的Sample
        purgeObsoleteSamples(config, now);
        float count = 0.0f;

        // 统计所有Sample中的事件数
        for (Sample sample : this.samples)
            count += sample.eventCount;
        if (count == 0.0f)
            return Double.NaN;

        // 总值
        float sum = 0.0f;
        float quant = (float) quantile;

        // 遍历桶
        for (int b = 0; b < buckets; b++) {
            // 遍历所有的Sample
            for (int s = 0; s < this.samples.size(); s++) {
                // 转换Sample类型
                HistogramSample sample = (HistogramSample) this.samples.get(s);
                // 获取所有的统计值
                float[] hist = sample.histogram.counts();
                // 统计值累加
                sum += hist[b];

                // 均值计算
                if (sum / count > quant)
                    return binScheme.fromBin(b);
            }
        }
        return Double.POSITIVE_INFINITY;
    }

    public double combine(List<Sample> samples, MetricConfig config, long now) {
        return value(config, now, 0.5);
    }

    @Override
    protected HistogramSample newSample(long timeMs) {
        return new HistogramSample(this.binScheme, timeMs);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long timeMs) {
        // 强转为HistogramSample并使用其内部的Histogram进行记录
        HistogramSample hist = (HistogramSample) sample;
        // 使用Histogram对象的record()方法
        hist.histogram.record(value);
    }

    private static class HistogramSample extends SampledStat.Sample {
        // 内部记录了Histogram对象
        private final Histogram histogram;

        private HistogramSample(BinScheme scheme, long now) {
            super(0.0, now);
            // 初始化Histogram对象
            this.histogram = new Histogram(scheme);
        }
    }

}
