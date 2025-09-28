package org.apache.flink.streaming.tutorial.source.split;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    /*
        使用 SideOutput 进行数据分流
     */

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration());
        env.setParallelism(2);

        DataStream<WaterSensor> sensorStream = env.socketTextStream("localhost", 30099).map((MapFunction<String, WaterSensor>) value -> {
            String[] splits = value.split(",");
            return new WaterSensor(splits[0].trim(), Long.parseLong(splits[1].trim()), Integer.parseInt(splits[2].trim()));
        });

        // 侧输出标签
        OutputTag<WaterSensor> s1Tag = new OutputTag<>(
                "s1",
                Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>(
                "s2",
                Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> processed = sensorStream.process(new ProcessFunction<>() {
            @Override
            public void processElement(
                    WaterSensor value,
                    ProcessFunction<WaterSensor, WaterSensor>.Context ctx,
                    Collector<WaterSensor> out) {
                // id 为 s1, s2 的数据，分别输出到对应的侧流
                switch (value.getId()){
                    case "s1":
                        ctx.output(s1Tag, value);
                        break;
                    case "s2":
                        ctx.output(s2Tag, value);
                        break;
                    default:
                        out.collect(value);
                }
            }
        });

        processed.print("主流");

        processed.getSideOutput(s1Tag).print("s1支流");
        processed.getSideOutput(s2Tag).print("s2支流");

        env.execute();
    }
}
