package org.apache.flink.streaming.tutorial.source.partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {

    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration());
        env.setParallelism(4);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 30099);

        // shuffle 随机分区
        // socketTextStream.shuffle().print();

        // rebalance 轮询分区
        // socketTextStream.rebalance().print();

        // broadcast 广播
        socketTextStream.broadcast().print();

        env.execute();
    }
}
