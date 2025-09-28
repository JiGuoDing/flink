package org.apache.flink.streaming.tutorial.source.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /*
        fromSource 写法
        env.fromSource(
            source,                   // SourceFunction(Source 的实现类)
            watermarkStrategy,        // Watermark 生成策略
            sourceName                // Source 名称
         */
        // 从文件读取数据
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("src/main/java/org/apache/flink/streaming/tutorial/resources/sql.py"))
                .build();

        DataStreamSource<String> fileStreamSource = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "FileSource");

        fileStreamSource.print();

        env.execute();
    }
}
