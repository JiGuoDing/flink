package org.apache.flink.streaming.tutorial.source.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;
import org.apache.flink.streaming.tutorial.source.function.WaterSensorMapFunction;

public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration());
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorStream = env.socketTextStream("localhost", 30099)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedSensorStream = sensorStream.keyBy(WaterSensor::getId);

        // 1. 指定窗口分配器(时间/技术/滚动/滑动/会话)
        // 没有 keyby 的窗口，窗口内的所有数据进入同一个子任务，并行度只能为1
        // sensorStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))); // 一个 10 秒的滚动窗口
        // 有 keyby 的窗口，每个 key 上都定义了一组窗口，各自独立地进行统计计算
        keyedSensorStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))); // 一个 10 秒的滑动窗口，滑动步长为 5 秒
        // keyedSensorStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))); // 一个会话窗口，静止间隔为 10 秒

        // keyedSensorStream.countWindow(5); // 一个基于元素个数的滚动窗口，窗口大小为 5 个元素
        // keyedSensorStream.countWindow(5, 2); // 一个基于元素个数的滑动窗口，窗口大小为 5 个元素，滑动步长为 2 个元素


        // 2. 指定窗口函数()
        WindowedStream<WaterSensor, String, TimeWindow> windowedKeyedSensorStream = keyedSensorStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 增量聚合：来一条数据，计算一条数据，窗口触发时输出计算结果

        // 全窗口函数：数据到达，先缓存数据，等到窗口触发时，一次性把数据拿出来，做统一计算并输出结果

        env.execute();
    }
}
