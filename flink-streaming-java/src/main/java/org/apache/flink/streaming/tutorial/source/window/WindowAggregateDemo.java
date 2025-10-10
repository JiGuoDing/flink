package org.apache.flink.streaming.tutorial.source.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;
import org.apache.flink.streaming.tutorial.source.function.WaterSensorMapFunction;

public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorStream = env.socketTextStream("localhost", 30099)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedSensorStream = sensorStream.keyBy(WaterSensor::getId);

        // 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> windowedKeyedStream = keyedSensorStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(5)));
        // 窗口函数：增量聚合 Aggregate，属于本窗口的第一条数据到达时，创建窗口、累加器；增量聚合，到达一条记录计算一条记录，调用一次 add 方法；
        // 窗口输出时调用一次 getResult 方法；输入、中间累加器、输出类型可以不一样，比较灵活。
        // 范型1：输入数据类型
        // 范型2：累加器数据类型
        // 范型3：输出数据类型
        SingleOutputStreamOperator<String> aggregatedStream = windowedKeyedStream.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
            /**
             * 初始化累加器
             * @return 累加器初始值
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("Initialize accumulator");
                return 0;
            }

            /**
             * 聚合逻辑
             * @param value 输入数据
             * @param accumulator 累加器
             * @return 新的累加器
             */
            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                System.out.println("Add data" + value + " to accumulator");
                return accumulator + value.getVc();
            }

            /**
             * 窗口闭合时，输出结果
             * @param accumulator 累加器
             * @return 输出结果
             */
            @Override
            public String getResult(Integer accumulator) {
                System.out.println("Get result from accumulator");
                return accumulator.toString();
            }

            /**
             * 合并累加器，只有会话窗口才会用到
             * @param a
             * @param b
             * @return 合并后的累加器
             */
            @Override
            public Integer merge(Integer a, Integer b) {
                return 0;
            }
        });

        aggregatedStream.print();

        env.execute();
    }
}
