package org.apache.flink.streaming.tutorial.source.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectKeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Tuple2<Integer, String>> stream1 = env.fromElements(
                Tuple2.of(1, "a1"), Tuple2.of(1, "a2"), Tuple2.of(2, "b"), Tuple2.of(3, "c"));
        stream1.print("stream1");

        DataStreamSource<Tuple3<Integer, String, Integer>> stream2 = env.fromElements(
                Tuple3.of(1, "aa1", 1), Tuple3.of(1, "aa2", 2), Tuple3.of(2, "bb", 1), Tuple3.of(3, "cc", 1));
        stream2.print("stream2");

        SingleOutputStreamOperator<String> coProcess = stream1
                .connect(stream2)
                // 多并行度下需要 keyBy 才能保证同 key 数据进入相同的下游，才能匹配上
                .keyBy(s1 -> s1.f0, s2 -> s2.f0)
                .process(new CoProcessFunction<>() {

                             // 模拟流的状态，暂存尚未匹配上的数据记录
                             final Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
                             final Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

                             /**
                              * 第一条流的处理逻辑
                              *
                              * @param value 数据记录
                              * @param ctx 上下文
                              * @param out 采集器
                              */
                             @Override
                             public void processElement1(
                                     Tuple2<Integer, String> value,
                                     CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx,
                                     Collector<String> out) {
                                 // 将数据放入缓存中
                                 Integer key = value.f0;
                                 s1Cache.computeIfAbsent(key, k -> new ArrayList<>()).add(value);

                                 // 从 s2Cache 中查找是否有 key 匹配的数据
                                    s2Cache.computeIfPresent(key, (k, v) -> {
                                        v.forEach(tup -> out.collect("s1: " + value + " <=> " + "s2: " + tup));
                                        return v;
                                    });
                             }

                             /**
                              * 第二条流的处理逻辑
                              *
                              * @param value 数据记录
                              * @param ctx 上下文
                              * @param out 采集器
                              */
                             @Override
                             public void processElement2(
                                     Tuple3<Integer, String, Integer> value,
                                     CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx,
                                     Collector<String> out) {
                                 // 将数据放入缓存中
                                 Integer key = value.f0;
                                 s2Cache.computeIfAbsent(key, k -> new ArrayList<>()).add(value);

                                 // 从 s1Cache 中查找是否有 key 匹配的数据
                                 s1Cache.computeIfPresent(key, (k, v) -> {
                                     v.forEach(tup -> out.collect("s1: " + tup + " <=> " + "s2: " + value));
                                     return v;
                                 });
                             }
                         }
                );

        coProcess.print("coProcessedStream");

        env.execute();
    }
}
