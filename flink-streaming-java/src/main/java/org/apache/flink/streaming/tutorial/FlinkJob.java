package org.apache.flink.streaming.tutorial;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkJob {
    public static void main(String[] args) throws Exception {
        /*
         使用 createLocalEnvironmentWithWebUI api 可以直接启动一个带有 Web UI 的本地 Flink 环境，直接运行 Flink 任务
         引入依赖 flink-web-runtime-web
         在 IDEA 中运行后，可以通过浏览器访问 http://localhost:8081 查看 Web UI，并行度如果不设置，默认为 CPU 线程数
         */
        // StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
        //         new Configuration());
        /*
        sign-off标记可以在提交界面右下角的设置中打开
         */

        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // 指定运行模式，BATCH/STREAMING，流批一体，用同一套 api
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        /*
        全局禁用算子链
        localEnvironmentWithWebUI.disableOperatorChaining();
        也可对单个算子进行禁用算子链
         */

        /*
        并行度的优先级：
            算子(代码中) > environment(env) > 提交时指定的 --parallelism > 配置文件 parallelism.default
         */

        // DataStreamSource<String> socketDS = localEnvironmentWithWebUI.socketTextStream(
        //         "210.28.132.20",
        //         7877);

        env.execute();
    }
}
