package org.apache.flink.streaming.tutorial.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;
import org.apache.flink.util.Collector;

public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> streamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // FlatMap 算子，一进多出
        SingleOutputStreamOperator<String> flattedStream = streamSource.flatMap(
                new FlatMapFunction<WaterSensor, String>() {
                    @Override
                    public void flatMap(WaterSensor value, Collector<String> out) {
                        if ("s1".equals(value.getId())){
                            out.collect(String.valueOf(value.getVc()));
                        } else if ("s2".equals(value.getId())){
                            out.collect(String.valueOf(value.getTs()));
                            out.collect(String.valueOf(value.getVc()));
                        }
                    }
                }
        );

        flattedStream.print();

        env.execute();
    }
}
