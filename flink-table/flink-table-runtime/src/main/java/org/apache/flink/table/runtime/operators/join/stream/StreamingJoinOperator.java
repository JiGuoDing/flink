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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

/** Streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN. */
public class StreamingJoinOperator extends AbstractStreamingJoinOperator {

    private static final long serialVersionUID = -376944622236540545L;

    // whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
    protected final boolean leftIsOuter;
    // whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
    protected final boolean rightIsOuter;

    private transient JoinedRowData outRow;
    private transient RowData leftNullRow;
    private transient RowData rightNullRow;

    // left join state
    protected transient JoinRecordStateView leftRecordStateView;
    // right join state
    protected transient JoinRecordStateView rightRecordStateView;

    public StreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                leftStateRetentionTime,
                rightStateRetentionTime);
        this.leftIsOuter = leftIsOuter;
        this.rightIsOuter = rightIsOuter;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.outRow = new JoinedRowData();
        this.leftNullRow = new GenericRowData(leftType.toRowSize());
        this.rightNullRow = new GenericRowData(rightType.toRowSize());

        // initialize states
        if (leftIsOuter) {
            this.leftRecordStateView =
                    OuterJoinRecordStateViews.create(
                            getRuntimeContext(),
                            "left-records",
                            leftInputSideSpec,
                            leftType,
                            leftStateRetentionTime);
        } else {
            this.leftRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "left-records",
                            leftInputSideSpec,
                            leftType,
                            leftStateRetentionTime);
        }

        if (rightIsOuter) {
            this.rightRecordStateView =
                    OuterJoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            rightStateRetentionTime);
        } else {
            this.rightRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            rightStateRetentionTime);
        }
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true, false);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false, false);
    }

    /**
     * Process an input element and output incremental joined records, retraction messages will be
     * sent in some scenarios.
     *
     * <p>Following is the pseudo code to describe the core logic of this method. The logic of this
     * method is too complex, so we provide the pseudo code to help understand the logic. We should
     * keep sync the following pseudo code with the real logic of the method.
     *
     * <p>Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents "UPDATE_AFTER",
     * "-U" represents "UPDATE_BEFORE". We forward input RowKind if it is inner join, otherwise, we
     * always send insert and delete for simplification. We can optimize this to send -U & +U
     * instead of D & I in the future (see FLINK-17337). They are equivalent in this join case. It
     * may need some refactoring if we want to send -U & +U, so we still keep -D & +I for now for
     * simplification. See {@code
     * FlinkChangelogModeInferenceProgram.SatisfyModifyKindSetTraitVisitor}.
     *
     * <pre>
     * if input record is accumulate
     * |  if input side is outer
     * |  |  if there is no matched rows on the other side, send +I[record+null], state.add(record, 0)
     * |  |  if there are matched rows on the other side
     * |  |  | if other side is outer
     * |  |  | |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  | |  if the matched num in the matched rows > 0, skip
     * |  |  | |  otherState.update(other, old + 1)
     * |  |  | endif
     * |  |  | send +I[record+other]s, state.add(record, other.size)
     * |  |  endif
     * |  endif
     * |  if input side not outer
     * |  |  state.add(record)
     * |  |  if there is no matched rows on the other side, skip
     * |  |  if there are matched rows on the other side
     * |  |  |  if other side is outer
     * |  |  |  |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  |  |  if the matched num in the matched rows > 0, skip
     * |  |  |  |  otherState.update(other, old + 1)
     * |  |  |  |  send +I[record+other]s
     * |  |  |  else
     * |  |  |  |  send +I/+U[record+other]s (using input RowKind)
     * |  |  |  endif
     * |  |  endif
     * |  endif
     * endif
     *
     * if input record is retract
     * |  state.retract(record)
     * |  if there is no matched rows on the other side
     * |  | if input side is outer, send -D[record+null]
     * |  endif
     * |  if there are matched rows on the other side, send -D[record+other]s if outer, send -D/-U[record+other]s if inner.
     * |  |  if other side is outer
     * |  |  |  if the matched num in the matched rows == 0, this should never happen!
     * |  |  |  if the matched num in the matched rows == 1, send +I[null+other]
     * |  |  |  if the matched num in the matched rows > 1, skip
     * |  |  |  otherState.update(other, old - 1)
     * |  |  endif
     * |  endif
     * endif
     * </pre>
     *
     * @param input the input element
     * @param inputSideStateView state of input side
     * @param otherSideStateView state of other side
     * @param inputIsLeft whether input side is left side
     * @param isSuppress whether suppress the output of redundant messages when the other side is
     *     outer join. This only applies to the case of mini-batch.
     */
    protected void processElement(
            RowData input,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft,
            boolean isSuppress)
            throws Exception {
        boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
        boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

        // 查找另一侧状态中与当前输入匹配的所有记录
        AssociatedRecords associatedRecords =
                AssociatedRecords.of(input, inputIsLeft, otherSideStateView, joinCondition);

        if (isAccumulateMsg) { // record is accumulate（插入 / 累加）
            if (inputIsOuter) { // input side is outer 输入侧是 outer
                OuterJoinRecordStateView inputSideOuterStateView =
                        (OuterJoinRecordStateView) inputSideStateView;
                if (associatedRecords.isEmpty()) { // there is no matched rows on the other side 若另一侧无匹配
                    // send +I[record+null] 输出 +I[input + null]
                    outRow.setRowKind(RowKind.INSERT);
                    outputNullPadding(input, inputIsLeft);
                    // state.add(record, 0) 保存该记录并把匹配数（associations）设置为0
                    inputSideOuterStateView.addRecord(input, 0);
                } else { // there are matched rows on the other side 若另一侧有匹配
                    if (otherIsOuter) { // other side is outer 另一侧流也是 outer，需要考虑 null-padding
                        // ? 为何要 cast 为 OuterJoinRecordStateView
                        OuterJoinRecordStateView otherSideOuterStateView =
                                (OuterJoinRecordStateView) otherSideStateView;
                        // 遍历另一侧与当前输入记录匹配的的所有记录
                        for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                            RowData other = outerRecord.record;
                            // if the matched num in the matched rows == 0 若另一侧记录的匹配次数为 0 -> 先撤回之前输出的 [null + other]
                            if (outerRecord.numOfAssociations == 0 && !isSuppress) {
                                // send -D[null+other] 发送 -D[null + other] 撤回之前的 null-padding
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(other, !inputIsLeft);
                            } // ignore matched number > 0 若匹配次数 > 0 则忽略
                            // otherState.update(other, old + 1) 更新另一侧状态中匹配记录的匹配次数 + 1
                            otherSideOuterStateView.updateNumOfAssociations(
                                    other, outerRecord.numOfAssociations + 1);
                        }
                    }
                    // 将 RowKind 设为 INSERT，准备输出新的匹配结果
                    outRow.setRowKind(RowKind.INSERT);
                    // send +I[record+other]s
                    // 输出 +I[input + other]s （所有匹配的另一侧记录）
                    for (RowData other : associatedRecords.getRecords()) {
                        // 把左右两侧 RowData 填入一个可复用的 JoinedRowData 对象（outRow）中，
                        // 通过 collector 发出到 Flink 数据流中，后续由运行时封装为 StreamRecord 并发送到下游算子
                        output(input, other, inputIsLeft);
                    }
                    // state.add(record, other.size)
                    // 输入侧状态中添加该记录，标记匹配次数为 “匹配到的记录数”
                    inputSideOuterStateView.addRecord(input, associatedRecords.size());
                }
            } else { // input side not outer 输入侧不是 outer
                // state.add(record)
                // ? 非 outer 侧直接加入状态（无需记录匹配次数）
                inputSideStateView.addRecord(input);
                if (!associatedRecords.isEmpty()) { // if there are matched rows on the other side 仅当另一侧有匹配记录时，才输出结果
                    if (otherIsOuter) { // if other side is outer 另一侧是 outer，需要考虑 null-padding
                        OuterJoinRecordStateView otherSideOuterStateView =
                                (OuterJoinRecordStateView) otherSideStateView;
                        // 遍历另一侧与当前输入记录匹配的的所有记录
                        for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                            // 若 outerRecord 匹配次数为 0 -> 先撤回之前输出的 [null + other]
                            if (outerRecord.numOfAssociations == 0
                                    && !isSuppress) { // if the matched num in the matched rows == 0
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(outerRecord.record, !inputIsLeft);
                            }
                            // otherState.update(other, old + 1)
                            // 更新 outerRecord 的匹配次数 +1
                            otherSideOuterStateView.updateNumOfAssociations(
                                    outerRecord.record, outerRecord.numOfAssociations + 1);
                        }
                        // send +I[record+other]s
                        // 将 RowKind 设为 INSERT，准备输出新的匹配结果
                        outRow.setRowKind(RowKind.INSERT);
                    } else { // 另一侧也不是 outer （inner join）
                        // send +I/+U[record+other]s (using input RowKind)
                        // ? 为何要使用输入的 RowKind
                        outRow.setRowKind(inputRowKind);
                    }
                    for (RowData other : associatedRecords.getRecords()) {
                        // 把左右两侧 RowData 填入一个可复用的 JoinedRowData 对象（outRow）中，
                        // 通过 collector 发出到 Flink 数据流中，后续由运行时封装为 StreamRecord 并发送到下游算子
                        output(input, other, inputIsLeft);
                    }
                }
                // skip when there is no matched rows on the other side 无匹配时不输出
            }
        } else { // input record is retract 从状态中撤回该记录
            // state.retract(record)
            if (!isSuppress) {
                inputSideStateView.retractRecord(input);
            }
            if (associatedRecords.isEmpty()) { // there is no matched rows on the other side 另一侧无匹配结果
                if (inputIsOuter) { // input side is outer 输入侧是 outer，考虑到 null-padding，撤回之前的 [input + null]
                    // send -D[record+null]
                    outRow.setRowKind(RowKind.DELETE);
                    outputNullPadding(input, inputIsLeft);
                }
                // nothing to do when input side is not outer 输入侧不是 outer 则无需处理（因为当前前提是另一侧没有与 input 匹配的记录）
            } else { // there are matched rows on the other side 另一侧有与 input 匹配的记录
                if (inputIsOuter) { // 如果输入侧是 outer，需要撤回之前输出的 [input + other] 结果
                    // send -D[record+other]s
                    outRow.setRowKind(RowKind.DELETE);
                } else { // 如果输入侧不是 outer，则发送 -D/-U[record + other]s （因为当前前提是另一侧有与 input 匹配的记录）
                    // send -D/-U[record+other]s (using input RowKind)
                    outRow.setRowKind(inputRowKind);
                }
                // 遍历所有的匹配结果，输出撤回结果
                for (RowData other : associatedRecords.getRecords()) {
                    output(input, other, inputIsLeft);
                }
                // if other side is outer 若另一侧是 outer -> 恢复其 null-padding
                if (otherIsOuter) {
                    OuterJoinRecordStateView otherSideOuterStateView =
                            (OuterJoinRecordStateView) otherSideStateView;
                    for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                        if (outerRecord.numOfAssociations == 1 && !isSuppress) {
                            // send +I[null+other]
                            // 若另一侧记录的匹配次数为 1 -> 撤回后无匹配，恢复其 null-padding 输出 +I[null + other]
                            outRow.setRowKind(RowKind.INSERT);
                            outputNullPadding(outerRecord.record, !inputIsLeft);
                        } // nothing else to do when number of associations > 1 若匹配次数 > 1 则无需处理
                        // otherState.update(other, old - 1)
                        // 更新另一侧记录的匹配次数 -1
                        otherSideOuterStateView.updateNumOfAssociations(
                                outerRecord.record, outerRecord.numOfAssociations - 1);
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------------------

    private void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
        if (inputIsLeft) {
            outRow.replace(inputRow, otherRow);
        } else {
            outRow.replace(otherRow, inputRow);
        }
        // outRow 是一个可复用的 JoinedRowData 对象
        collector.collect(outRow);
    }

    private void outputNullPadding(RowData row, boolean isLeft) {
        if (isLeft) {
            outRow.replace(row, rightNullRow);
        } else {
            outRow.replace(leftNullRow, row);
        }
        collector.collect(outRow);
    }
}
