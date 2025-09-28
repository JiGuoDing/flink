package org.apache.flink.streaming.tutorial.source.operator;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> streamSource1 = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 3L, 3),
                new WaterSensor("s2", 4L, 4),
                new WaterSensor("s3", 5L, 5));

        DataStreamSource<WaterSensor> streamSource2 = env.fromElements(
                new WaterSensor("s3", 6L, 6),
                new WaterSensor("s4", 7L, 7),
                new WaterSensor("s4", 8L, 8),
                new WaterSensor("s5", 9L, 9),
                new WaterSensor("s6", 0L, 0));

        DataStream<WaterSensor> unitedStream = streamSource1.union(streamSource2);
        ConnectedStreams<WaterSensor,WaterSensor> connectedStream = streamSource1.connect(streamSource2);

        unitedStream.print();
        
        env.execute();
    }
}
