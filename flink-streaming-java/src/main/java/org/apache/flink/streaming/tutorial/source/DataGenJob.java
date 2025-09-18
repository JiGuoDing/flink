package org.apache.flink.streaming.tutorial.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        /*
            数据生成器 DataGeneratorSource，涉及 4 个参数
            1. GeneratorFunction：数据生成函数，需要重写其中的 map 方法，其又涉及 2 个参数
                1. 索引，从 0 开始自增
                2. 返回值，生成的数据
            2. Number of records：生成数据的总条数
            3. RateLimiterStrategy：限流策略
            4. TypeInformation：数据类型信息

             如果有 n 的并行度，最大值为 a，则会将数值均分成 n 份，每份 a/n
         */
        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, String>) aLong -> "Number: " + aLong,
                100,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );

        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "DataGenSource").print();

        env.execute();
    }
}
