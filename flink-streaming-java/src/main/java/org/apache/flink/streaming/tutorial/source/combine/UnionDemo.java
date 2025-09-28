package org.apache.flink.streaming.tutorial.source.combine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        stream1.print("源1");
        DataStreamSource<Integer> stream2 = env.fromElements(4, 5, 6);
        stream2.print("源2");
        DataStreamSource<String> stream3 = env.fromElements("7", "8", "9");
        stream3.print("源3");

        DataStream<Integer> union = stream1.union(stream2).union(stream3.map(Integer::valueOf));

        union.print("合流");
        env.execute();
    }
}
