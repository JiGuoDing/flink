package org.apache.flink.streaming.tutorial.source.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;
import org.apache.flink.streaming.tutorial.source.function.WaterSensorMapFunction;

public class WindowReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorStream = env.socketTextStream("localhost", 30099)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedSensorStream = sensorStream.keyBy(WaterSensor::getId);

        // 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> windowedKeyedStream = keyedSensorStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(5)));
        // 窗口函数
        SingleOutputStreamOperator<WaterSensor> reduceStream = windowedKeyedStream.reduce((ReduceFunction<WaterSensor>) (value1, value2) -> {
            System.out.println("调用 reduce 方法... {} + {}");
            return new WaterSensor(
                    value1.getId(),
                    value2.getTs(),
                    value1.getVc() + value2.getVc());
        });

        reduceStream.print();

        env.execute();
    }
}
