/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.socket;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * <p>This program connects to a server socket and reads strings from the socket. The easiest way to
 * try this out is to open a text server (at port 12345) using the <i>netcat</i> tool via
 *
 * <pre>
 * nc -l 12345 on Linux or nc -l -p 12345 on Windows
 * </pre>
 *
 * <p>and run this example with the hostname and the port as arguments.
 */
public class SocketWindowWordCount {

    private static Logger logger = LoggerFactory.getLogger(SocketWindowWordCount.class);

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println(
                    "No port specified. Please run 'SocketWindowWordCount "
                            + "--hostname <hostname> --port <port>', where hostname (localhost by default) "
                            + "and port is the address of the text server");
            System.err.println(
                    "To start a simple text server, run 'netcat -l <port>' and "
                            + "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts =
                text.flatMap(
                                (FlatMapFunction<String, WordWithCount>)
                                        (value, out) -> {
                                            for (String word : value.split("\\s")) {
                                                out.collect(new WordWithCount(word, 1L));
                                            }
                                        },
                                Types.POJO(WordWithCount.class))
                        .keyBy(value -> value.word)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                        // aggregate 聚合方式
                        .aggregate(
                                new AggregateFunction<
                                        WordWithCount, WordWithCount, WordWithCount>() {

                                    @Override
                                    public WordWithCount createAccumulator() {
                                        logger.info("createAccumulator invoked");
                                        // initialize with empty word and zero count
                                        return new WordWithCount("", 0L);
                                    }

                                    /*
                                       把单个输入合并到累加器(acc)——按元素到达时间调用，做增量更新
                                    */
                                    @Override
                                    public WordWithCount add(
                                            WordWithCount value, WordWithCount accumulator) {
                                        // set the word if this is the first element
                                        if (accumulator.word == null
                                                || accumulator.word.isEmpty()) {
                                            accumulator.word = value.word;
                                        }
                                        accumulator.count += value.count;
                                        return accumulator;
                                    }

                                    @Override
                                    public WordWithCount getResult(WordWithCount accumulator) {
                                        return accumulator;
                                    }

                                    /*
                                       把两个累加器合并成一个——在需要合并状态时调用。
                                    */
                                    @Override
                                    public WordWithCount merge(WordWithCount a, WordWithCount b) {
                                        String word =
                                                (a.word != null && !a.word.isEmpty())
                                                        ? a.word
                                                        : b.word;
                                        long count = (a.count) + (b.count);
                                        return new WordWithCount(word, count);
                                    }
                                });

        // reduce 聚合方式
        // .reduce((a, b) -> new WordWithCount(a.word, a.count + b.count))
        // .returns(WordWithCount.class);

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // ------------------------------------------------------------------------

    /** Data type for words with count. */
    public static class WordWithCount {

        public String word;
        public long count;

        @SuppressWarnings("unused")
        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
