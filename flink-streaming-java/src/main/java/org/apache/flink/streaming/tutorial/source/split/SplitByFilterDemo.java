package org.apache.flink.streaming.tutorial.source.split;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration());
        env.setParallelism(2);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 30099);

        // 偶数分流
        socketTextStream.filter( value -> Integer.parseInt(value) % 2 == 0).print("偶数流");

        // 奇数分流
        socketTextStream.filter(value -> Integer.parseInt(value) % 2 == 1).print("奇数流");

        /*
            缺点：同一个数据要被处理两次
         */

        env.execute();
    }
}
