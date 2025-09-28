package org.apache.flink.streaming.tutorial.source.partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomPartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration());
        env.setParallelism(2);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 30099);

        // 自定义分区器
        socketTextStream.partitionCustom(new JCustomPartitioner(), str -> str).print();

        env.execute();
    }
}
