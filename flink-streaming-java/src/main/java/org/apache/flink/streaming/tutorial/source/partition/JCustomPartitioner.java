package org.apache.flink.streaming.tutorial.source.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区器，简单取模进行分区
 */
public class JCustomPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
