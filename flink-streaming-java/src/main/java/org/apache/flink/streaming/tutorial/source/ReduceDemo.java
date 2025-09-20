package org.apache.flink.streaming.tutorial.source;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;

public class ReduceDemo {
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
            Reduce 算子
                1. 需要在 KeyedStream 上使用
                2. 只能对同类型数据进行聚合计算
         */
        SingleOutputStreamOperator<WaterSensor> reducedStream = keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(
                        value1.getId(),
                        value1.getTs(),
                        value1.getVc() + value2.getVc());
            }
        });

        reducedStream.print();

        env.execute();
    }
}
