/**
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

package org.apache.tajo.worker;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.common.ProtoObject;

import java.util.Collections;
import java.util.Map;

import static org.apache.tajo.ipc.TajoWorkerProtocol.TaskHistoryProto;
import static org.apache.tajo.ipc.TajoWorkerProtocol.TaskRunnerHistoryProto;

/**
 * The history class for TaskRunner processing.
 */
public class TaskRunnerHistory implements ProtoObject<TaskRunnerHistoryProto> {

  private TaskRunner.STATE state;
  private TaskRunnerId taskRunnerId;
  private long startTime;
  private long finishTime;
  private ExecutionBlockId executionBlockId;
  private Map<QueryUnitAttemptId, TaskHistory> taskHistoryMap = null;

  public TaskRunnerHistory(TaskRunnerId taskRunnerId, ExecutionBlockId executionBlockId) {
    init();
    this.taskRunnerId = taskRunnerId;
    this.executionBlockId = executionBlockId;
  }

  public TaskRunnerHistory(TaskRunnerHistoryProto proto) {
    this.state = TaskRunner.STATE.valueOf(proto.getState());
    this.taskRunnerId = new TaskRunnerId(proto.getTaskRunnerId());
    this.startTime = proto.getStartTime();
    this.finishTime = proto.getFinishTime();
    this.executionBlockId = new ExecutionBlockId(proto.getExecutionBlockId());
    this.taskHistoryMap = Maps.newTreeMap();
    for (TaskHistoryProto taskHistoryProto : proto.getTaskHistoriesList()) {
      TaskHistory taskHistory = new TaskHistory(taskHistoryProto);
      taskHistoryMap.put(taskHistory.getQueryUnitAttemptId(), taskHistory);
    }
  }

  private void init() {
    this.taskHistoryMap = Maps.newHashMap();
  }

  public int size() {
    return this.taskHistoryMap.size();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(taskRunnerId, executionBlockId, state, startTime,
        finishTime, taskHistoryMap.size());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TaskRunnerHistory) {
      TaskRunnerHistory other = (TaskRunnerHistory) o;
      boolean result = getProto().equals(other.getProto());
      if (!result) {
        System.out.println("Origin TaskRunnerHistory:" + getProto().toString());
        System.out.println("Target TaskRunnerHistory:" + other.getProto().toString());
      }
      return result;
    }
    return false;
  }

  @Override
  public TaskRunnerHistoryProto getProto() {
    TaskRunnerHistoryProto.Builder builder = TaskRunnerHistoryProto.newBuilder();
    builder.setTaskRunnerId(taskRunnerId.getProto());
    builder.setState(state.toString());
    builder.setExecutionBlockId(executionBlockId.getProto());
    builder.setStartTime(startTime);
    builder.setFinishTime(finishTime);
    for (TaskHistory taskHistory : taskHistoryMap.values()){
      builder.addTaskHistories(taskHistory.getProto());
    }
    return builder.build();
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public TaskRunner.STATE getState() {
    return state;
  }

  public void setState(TaskRunner.STATE state) {
    this.state = state;
  }

  public TaskRunnerId getTaskRunnerId() {
    return taskRunnerId;
  }

  public void setTaskRunnerId(TaskRunnerId taskRunnerId) {
    this.taskRunnerId = taskRunnerId;
  }

  public TaskHistory getTaskHistory(QueryUnitAttemptId queryUnitAttemptId) {
    return taskHistoryMap.get(queryUnitAttemptId);
  }

  public Map<QueryUnitAttemptId, TaskHistory> getTaskHistoryMap() {
    return Collections.unmodifiableMap(taskHistoryMap);
  }

  public void addTaskHistory(QueryUnitAttemptId queryUnitAttemptId, TaskHistory taskHistory) {
    taskHistoryMap.put(queryUnitAttemptId, taskHistory);
  }
}
