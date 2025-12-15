/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The {@link StateExecutor} implementation which executing batch {@link StateRequest}s for
 * ForStStateBackend.
 */
public class ForStStateExecutor implements StateExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ForStStateExecutor.class);

    /**
     * The coordinator thread which schedules the execution of multiple batches of stateRequests.
     * The number of coordinator threads is 1 to ensure that multiple batches of stateRequests can
     * be executed sequentially.
     */
    private final ExecutorService coordinatorThread;

    /** The worker thread that actually executes the {@link StateRequest}s. */
    private final ExecutorService workerThreads;

    private final RocksDB db;

    private final WriteOptions writeOptions;

    private Throwable executionError;

    public ForStStateExecutor(int ioParallelism, RocksDB db, WriteOptions writeOptions) {
        this.coordinatorThread =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("ForSt-StateExecutor-Coordinator"));
        this.workerThreads =
                Executors.newFixedThreadPool(
                        ioParallelism, new ExecutorThreadFactory("ForSt-StateExecutor-IO"));
        this.db = db;
        this.writeOptions = writeOptions;
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            StateRequestContainer stateRequestContainer) {
        // 检查上一次执行是否发生错误 (若有则抛出，阻止新批次)
        checkState();
        Preconditions.checkArgument(stateRequestContainer instanceof ForStStateRequestClassifier);
        ForStStateRequestClassifier stateRequestClassifier =
                (ForStStateRequestClassifier) stateRequestContainer;
        // 返回给调用者的异步句柄 (立即返回，实际完成在 coordinator 线程)
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        // 在 coordinator 线程中执行批次请求 (保证批次顺序性)
        coordinatorThread.execute(
                () -> {
                    long startTime = System.currentTimeMillis();
                    // 用于收集所有子操作的 futures (例如 put 操作，get 操作)
                    List<CompletableFuture<Void>> futures = new ArrayList<>(2);
                    // 从分类器中获取需要写入 DB 的请求 (按 keyGroup 等已分类)
                    List<ForStDBPutRequest<?, ?>> putRequests =
                            stateRequestClassifier.pollDbPutRequests();
                    if (!putRequests.isEmpty()) {
                        // 构造写批次操作，实际 I/O 在 workerThreads 并发执行
                        ForStWriteBatchOperation writeOperations =
                                new ForStWriteBatchOperation(
                                        db, putRequests, writeOptions, workerThreads);
                        // 将写操作的 future 加入集合，后续统一组合
                        futures.add(writeOperations.process());
                    }

                    // 从分类器中获取需要从 DB 读取的请求
                    List<ForStDBGetRequest<?, ?>> getRequests =
                            stateRequestClassifier.pollDbGetRequests();
                    if (!getRequests.isEmpty()) {
                        // 构造多项并发 get 操作，实际 I/O 在 workerThreads 并发执行
                        ForStGeneralMultiGetOperation getOperations =
                                new ForStGeneralMultiGetOperation(db, getRequests, workerThreads);
                        // 将读操作的 future 添加到集合，稍后统一组合
                        futures.add(getOperations.process());
                    }

                    // * 将所有子 future 组合成一个整体 future
                    FutureUtils.combineAll(futures)
                            // 当所有子操作成功时，在 coordinator 线程中完成 resultFuture
                            .thenAcceptAsync(
                                    (e) -> {
                                        long duration = System.currentTimeMillis() - startTime;
                                        LOG.debug(
                                                "Complete executing a batch of state requests, putRequest size {}, getRequest size {}, duration {} ms",
                                                putRequests.size(),
                                                getRequests.size(),
                                                duration);
                                        resultFuture.complete(null);
                                    },
                                    coordinatorThread)
                            // 若任一子操作失败，则记录 executionError 并将异常传递给 resultFuture
                            .exceptionally(
                                    e -> {
                                        executionError = e;
                                        resultFuture.completeExceptionally(e);
                                        return null;
                                    });
                });
        // 方法立即返回，不阻塞调用者通过返回的CompletableFuture 获取执行结果
        return resultFuture;
    }

    @Override
    public StateRequestContainer createStateRequestContainer() {
        checkState();
        return new ForStStateRequestClassifier();
    }

    private void checkState() {
        if (executionError != null) {
            throw new IllegalStateException(
                    "previous state request already failed : ", executionError);
        }
    }

    @Override
    public void shutdown() {
        workerThreads.shutdown();
        coordinatorThread.shutdown();
        LOG.info("Shutting down the ForStStateExecutor.");
    }
}
