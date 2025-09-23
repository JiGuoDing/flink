package org.apache.flink.streaming.tutorial.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;

import java.util.Arrays;
import java.util.List;

public class MapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<WaterSensor> sensors = Arrays.asList(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // 不明确指定元素类型
        // DataStreamSource<WaterSensor> streamSource = env.fromElements(
        //         new WaterSensor("s1", 1L, 1),
        //         new WaterSensor("s2", 2L, 2),
        //         new WaterSensor("s3", 3L, 3)
        // );

        // 明确指定元素类型
        DataStreamSource<WaterSensor> streamSource = env.fromCollection(sensors, TypeInformation.of(
                WaterSensor.class));

        // map算子，一进一出
        // 方式1: lambda 表达式
        // SingleOutputStreamOperator<String> mapStream = streamSource.map((MapFunction<WaterSensor, String>) WaterSensor::getId);

        // 方式2: 匿名类
        /*
        SingleOutputStreamOperator<String> mapStream = streamSource.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
         */

        // 方式3: 自定义类来实现 MapFunction
        SingleOutputStreamOperator<String> mapStream = streamSource.map(new MyMapFunction());

        mapStream.print();

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<WaterSensor, String> {

        @Override
        public String map(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
}
