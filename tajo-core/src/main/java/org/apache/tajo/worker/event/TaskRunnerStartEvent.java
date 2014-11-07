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

package org.apache.tajo.worker.event;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;

public class TaskRunnerStartEvent extends TaskRunnerEvent {

  private final QueryContext queryContext;
  private final String plan;
  private final int tasks;
  private final WorkerConnectionInfo queryMaster;

  public TaskRunnerStartEvent(WorkerConnectionInfo queryMaster,
                              ExecutionBlockId executionBlockId,
                              int tasks,
                              QueryContext context,
                              String plan) {
    super(EventType.START, executionBlockId);
    this.queryMaster = queryMaster;
    this.tasks = tasks;
    this.queryContext = context;
    this.plan = plan;
  }

  public WorkerConnectionInfo getQueryMaster(){
    return queryMaster;
  }

  public int getTasks() {
    return tasks;
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  public String getPlan() {
    return plan;
  }
}
