package org.apache.flink.streaming.tutorial.source.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class SinkFileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 必须开启 Checkpoint，否则一致都是 inprogress，无法查看文件
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, String>) value -> "Number: " + value, Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.STRING);

        DataStreamSource<String> dataGen = env.fromSource(
                dataGeneratorSource,
                WatermarkStrategy.noWatermarks(),
                "data-generator-source");

        // 输出到文件系统
        // 输出行式存储的文件，指定路径、指定编码
        FileSink<String> fileSink = FileSink
                .<String>forRowFormat(
                        new Path("/Users/jiguoding/workspace/flink/flink-streaming-java/data/tmp"),
                        new SimpleStringEncoder<>("UTF-8")).
                // 输出文件的一些配置：文件的前缀、后缀
                        withOutputFileConfig(
                        OutputFileConfig.builder().
                                withPartPrefix("jgd").
                                withPartSuffix(".log").
                                build()
                ).
                // 按照目录进行文件分桶
                withBucketAssigner(new DateTimeBucketAssigner<>(
                        "yyyy-MM-dd hh",
                        ZoneId.systemDefault())).
                // 文件滚动策略：滚动时间为 10s，文件大小 1MB
                        withRollingPolicy(
                        DefaultRollingPolicy.builder().
                                withRolloverInterval(Duration.ofSeconds(10)).
                                withMaxPartSize(new MemorySize(1024 * 1024)).
                                build()
                )
                .build();

        dataGen.sinkTo(fileSink);

        env.execute();
    }
}
