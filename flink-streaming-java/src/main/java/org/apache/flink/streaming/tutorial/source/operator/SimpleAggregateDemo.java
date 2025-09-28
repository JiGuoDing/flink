package org.apache.flink.streaming.tutorial.source.operator;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;

public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> streamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> keyedStream = streamSource.keyBy(
                WaterSensor::getId);

        /*
            简单聚合算子需要在 KeyedStream 上使用；分组内的聚合，对同一个 key 的数据进行聚合
            两种指定字段方式：
                1. 字符串，字段名
                2. 索引，从0开始，并且只适用于 Tuple 类型, POJO 不行
         */
        // SingleOutputStreamOperator<WaterSensor> vcSum = keyedStream.sum("vc");
        // vcSum.print();

        // SingleOutputStreamOperator<WaterSensor> vcMin = keyedStream.min("vc");
        // vcMin.print();

        // SingleOutputStreamOperator<WaterSensor> vcMax = keyedStream.max("vc");
        // vcMax.print();

        /*
            MaxBy 和 Max 的区别
                Max 只会取比较字段的最大值，非比较字段保留第一次的值
                MaxBy 取比较字段的最大值，同时非比较字段也会取具有比较字段最大值的这条记录的值
         */
        SingleOutputStreamOperator<WaterSensor> vcMaxBy = keyedStream.maxBy("vc");
        vcMaxBy.print();

        env.execute();
    }
}
