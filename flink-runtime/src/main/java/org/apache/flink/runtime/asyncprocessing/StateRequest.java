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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.core.state.InternalStateFuture;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A request encapsulates the necessary data to perform a state request.
 *
 * <p>把 StateRequest 想成一个“快递包裹”：包裹外面写着目的地（RecordContext，即哪个 key）、
 * 包裹类型标签（type）、包内物品（payload）和一张回执单（stateFuture）。 配送员（状态执行器）接到包裹去投递，投递完成在回执上签字（完成 future），
 * 包裹处理完后回收包装并释放占用（disposer）。
 *
 * @param <K> Type of partitioned key.
 * @param <IN> Type of input of this request.
 * @param <OUT> Type of value that request will return.
 */
public class StateRequest<K, IN, OUT> implements Serializable {

    /**
     * The underlying state to be accessed, can be empty for {@link StateRequestType#SYNC_POINT}.
     */
    @Nullable private final State state;

    /** The type of this request. */
    private final StateRequestType type;

    /** The payload(input) of this request. */
    @Nullable private final IN payload;

    /** The future to collect the result of the request. */
    private final InternalStateFuture<OUT> stateFuture;

    /** The record context of this request. */
    private final RecordContext<K> context;

    public StateRequest(
            @Nullable State state,
            StateRequestType type,
            @Nullable IN payload,
            InternalStateFuture<OUT> stateFuture,
            RecordContext<K> context) {
        // 要访问的 state
        this.state = state;
        this.type = type;
        this.payload = payload;
        this.stateFuture = stateFuture;
        // RecordContext 是围绕单条记录封装的上下文与生命周期管理单元，负责保存该记录相关元数据，
        // 并在引用计数结束时安全地释放与键相关的占用。
        this.context = context;
    }

    public StateRequestType getRequestType() {
        return type;
    }

    @Nullable
    public IN getPayload() {
        return payload;
    }

    @Nullable
    public State getState() {
        return state;
    }

    public InternalStateFuture<OUT> getFuture() {
        return stateFuture;
    }

    public RecordContext<K> getRecordContext() {
        return context;
    }
}
