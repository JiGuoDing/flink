package org.apache.flink.streaming.tutorial.source.window;

import org.apache.commons.lang3.time.DateFormatUtils;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;
import org.apache.flink.streaming.tutorial.source.function.WaterSensorMapFunction;
import org.apache.flink.util.Collector;

public class FullWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorStream = env.socketTextStream("localhost", 30099)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedSensorStream = sensorStream.keyBy(WaterSensor::getId);

        // 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> windowedKeyedStream = keyedSensorStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> processedWindowedKeyedStream = windowedKeyedStream.process(
                new ProcessWindowFunction<>() {
                    /**
                     * 全窗口函数：窗口触发时调用一次，统一计算窗口的所有数据
                     * @param s 分组的 key
                     * @param context 上下文，可以获取窗口信息
                     * @param elements 属于窗口的已暂存数据
                     * @param out 采集器
                     */
                    @Override
                    public void process(
                            String s,
                            ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context,
                            Iterable<WaterSensor> elements,
                            Collector<String> out) {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();

                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss");

                        long elementCount = elements.spliterator().estimateSize();
                        out.collect("key="+s+"'s window ["+windowStart+" , "+windowEnd+") has "+elementCount+" records ===> " + elements);
                    }
                });

        processedWindowedKeyedStream.print();

        env.execute();
    }
}
