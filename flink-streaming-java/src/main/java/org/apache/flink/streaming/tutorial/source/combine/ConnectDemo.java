package org.apache.flink.streaming.tutorial.source.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        stream1.print("源1");
        DataStreamSource<String> stream2 = env.fromElements("a", "n", "t");
        stream2.print("源2");

        ConnectedStreams<Integer, String> connected = stream1.connect(stream2);

        SingleOutputStreamOperator<String> mapped = connected.map(new CoMapFunction<>() {
            @Override
            public String map1(Integer value) {
                return value.toString();
            }

            @Override
            public String map2(String value) {
                return value;
            }
        });

        mapped.print("合流");

        env.execute();
    }
}
