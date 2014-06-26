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

package org.apache.tajo.master.event;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.tajo.ExecutionBlockId;

public class WorkerResourceRequestEvent extends AbstractEvent<WorkerResourceRequestEvent.EventType> {

  public enum EventType {
    CONTAINER_RELEASE
  }

  private final int workerId;
  private final ExecutionBlockId executionBlockId;
  private final int resourceSize;

  public WorkerResourceRequestEvent(WorkerResourceRequestEvent.EventType eventType,
                                    ExecutionBlockId executionBlockId,
                                    int workerId,
                                    int resourceSize) {
    super(eventType);
    this.workerId = workerId;
    this.executionBlockId = executionBlockId;
    this.resourceSize = resourceSize;
  }

  public int getWorkerId() {
    return workerId;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public int getResourceSize() {
    return resourceSize;
  }
}
