package org.apache.flink.streaming.tutorial.source.job;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CollectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合接口读取数据
        // DataStreamSource<Integer> streamSource = env.fromCollection(Arrays.asList(
        //         1,
        //         2,
        //         3,
        //         4,
        //         5,
        //         6,
        //         7,
        //         8,
        //         9,
        //         10));

        // 直接给元素读取数据
        DataStreamSource<Object> streamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        streamSource.print();

        env.execute();
    }
}
