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

import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.v2.InternalKeyedState;

import org.junit.jupiter.api.Test;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link ForStStateExecutor}. */
public class ForStStateExecutorTest extends ForStDBOperationTestBase {

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteValueStateRequest() throws Exception {
        // * 创建 ForstStateExecutor (4 代表并行度或分片数)，并准备 RocksDB WriteOptions
        ForStStateExecutor forStStateExecutor = new ForStStateExecutor(4, db, new WriteOptions());
        ForStValueState<Integer, String> state1 = buildForStValueState("value-state-1");
        ForStValueState<Integer, String> state2 = buildForStValueState("value-state-2");

        StateRequestContainer stateRequestContainer =
                forStStateExecutor.createStateRequestContainer();
        assertTrue(stateRequestContainer.isEmpty());

        // 1. Update value state: keyRange [0, keyNum)
        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            // * 构造并放入 VALUE_UPDATE 请求
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, "test-" + i, i * 2));
        }

        // * 提交并执行第一批请求 (写入操作)
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        List<StateRequest<?, ?, ?>> checkList = new ArrayList<>();
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        // 2. Get value state: keyRange [0, keyNum)
        //    Update value state: keyRange [keyNum, keyNum + 100]
        for (int i = 0; i < keyNum; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            // * 构建并放入 VALUE_GET 请求
            StateRequest<?, ?, ?> getRequest =
                    buildStateRequest(state, StateRequestType.VALUE_GET, i, null, i * 2);
            stateRequestContainer.offer(getRequest);
            checkList.add(getRequest);
        }
        for (int i = keyNum; i < keyNum + 100; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            // * 构建并放入一批新的 VALUE_UPDATE 请求
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, "test-" + i, i * 2));
        }
        // * 执行第二批请求 (包含读和写)
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        // 3. Check value state Get result : [0, keyNum)
        // 校验所有之前的 GET 请求结果与预期一致 (已写入的 value)
        for (StateRequest<?, ?, ?> getRequest : checkList) {
            assertThat(getRequest.getRequestType()).isEqualTo(StateRequestType.VALUE_GET);
            int key = (Integer) getRequest.getRecordContext().getKey();
            // 校验 recordContext 中的 record 没被篡改，仍为 i*2
            assertThat(getRequest.getRecordContext().getRecord()).isEqualTo(key * 2);
            // 校验异步 future 返回的 value 等于写入的 "test-" + key
            assertThat(((TestStateFuture<String>) getRequest.getFuture()).getCompletedResult())
                    .isEqualTo("test-" + key);
        }

        // 4. Clear value state:  keyRange [keyNum - 100, keyNum)
        //    Update state with null-value : keyRange [keyNum, keyNum + 100]
        // 批量清除一段 key 的状态，并对另一段 key 写入 null (表示删除或清空)
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        for (int i = keyNum - 100; i < keyNum; i++) {
            //  对 [keyNum - 100, keyNum) 范围发布 CLEAR 请求
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.CLEAR, i, null, i * 2));
        }
        // 对 [keyNum, keyNum + 100) 范围发布 VALUE_UPDATE 请求，value 为 null（等效于清空）
        for (int i = keyNum; i < keyNum + 100; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, null, i * 2));
        }
        // * 执行清除和写入 null 的批次请求
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        // 5. Check that the deleted value is null :  keyRange [keyNum - 100, keyNum + 100)
        // 校验在 [keyNum - 100, keyNum + 100) 范围内的所有 key 的 value 已被清空（应为 null）
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        checkList.clear();
        for (int i = keyNum - 100; i < keyNum + 100; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            StateRequest<?, ?, ?> getRequest =
                    buildStateRequest(state, StateRequestType.VALUE_GET, i, null, i * 2);
            stateRequestContainer.offer(getRequest);
            checkList.add(getRequest);
        }
        // * 执行读取操作获取当前状态
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();
        for (StateRequest<?, ?, ?> getRequest : checkList) {
            assertThat(getRequest.getRequestType()).isEqualTo(StateRequestType.VALUE_GET);
            assertThat(((TestStateFuture<String>) getRequest.getFuture()).getCompletedResult())
                    .isEqualTo(null);
        }
        // * 释放 Executor 相关资源
        forStStateExecutor.shutdown();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <K, V, R> StateRequest<?, ?, ?> buildStateRequest(
            // 底层的键控状态实例，表示这次请求要操作的状态表/句柄
            InternalKeyedState<K, V> innerTable,
            // 请求类型枚举
            StateRequestType requestType,
            K key,
            // 请求携带的 payload (写入时为要写的 value，读/清除时可能为 null)
            V value,
            // 与该请求关联的输入记录，可能是业务记录或用于校验/跟踪的元数据
            R record) {
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 128);
        RecordContext<K> recordContext =
                new RecordContext<>(record, key, t -> {}, keyGroup, new Epoch(0));
        TestStateFuture stateFuture = new TestStateFuture<>();
        return new StateRequest<>(innerTable, requestType, value, stateFuture, recordContext);
    }
}
