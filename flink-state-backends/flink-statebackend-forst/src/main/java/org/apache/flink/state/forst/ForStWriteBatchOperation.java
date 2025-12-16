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

import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** The writeBatch operation implementation for ForStDB. */
public class ForStWriteBatchOperation implements ForStDBOperation {

    private static final int PER_RECORD_ESTIMATE_BYTES = 100;

    private final RocksDB db;

    private final List<ForStDBPutRequest<?, ?>> batchRequest;

    private final WriteOptions writeOptions;

    private final Executor executor;

    ForStWriteBatchOperation(
            RocksDB db,
            List<ForStDBPutRequest<?, ?>> batchRequest,
            WriteOptions writeOptions,
            Executor executor) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.writeOptions = writeOptions;
        this.executor = executor;
    }

    /**
     * process() 返回一个异步执行的 CompletableFuture，在给定的 executor 中将 batchRequest 中的请求打包成一个 RocksDB
     * 的WriteBatch 并一次性写入。如果写入成功，会彼为每个请求标记完成； 若发生异常，会为每个请求标记失败并让返回的 future 异常完成.
     *
     * <p>该方法本身非阻塞，调用者立即得到 CompletableFuture，实际 IO 在指定的 executor 线程执行.
     *
     * @return
     */
    @Override
    public CompletableFuture<Void> process() {
        return CompletableFuture.runAsync(
                () -> {
                    // 创建一个初始容量为 batchRequest.size() * PER_RECORD_ESTIMATE_BYTES 的 WriteBatch
                    try (WriteBatch writeBatch =
                            new WriteBatch(batchRequest.size() * PER_RECORD_ESTIMATE_BYTES)) {
                        // 遍历 batchRequest 中的每个 ForstDBPutRequest
                        for (ForStDBPutRequest<?, ?> request : batchRequest) {
                            if (request.valueIsNull()) {
                                // * put(key, null) == delete(key)：将 put(key, null) 视为删除
                                writeBatch.delete(
                                        request.getColumnFamilyHandle(),
                                        request.buildSerializedKey());
                            } else {
                                writeBatch.put(
                                        request.getColumnFamilyHandle(),
                                        request.buildSerializedKey(),
                                        request.buildSerializedValue());
                            }
                        }
                        // * 将整个 WriteBatch 一次性 (原子性) 写入 RocksDB
                        db.write(writeOptions, writeBatch);
                        // * 若 db.write() 成功，则为 batchRequest 中的每个请求标记完成
                        for (ForStDBPutRequest<?, ?> request : batchRequest) {
                            request.completeStateFuture();
                        }
                    } catch (Exception e) {
                        String msg = "Error while write batch data to ForStDB.";
                        for (ForStDBPutRequest<?, ?> request : batchRequest) {
                            // fail every state request in this batch
                            request.completeStateFutureExceptionally(msg, e);
                        }
                        // fail the whole batch operation
                        throw new CompletionException(msg, e);
                    }
                },
                executor);
    }
}
