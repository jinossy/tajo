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

package org.apache.tajo.scheduler;

import com.google.common.base.Objects;
import org.apache.tajo.QueryId;
import org.apache.tajo.master.session.Session;

import java.util.Set;

public class QuerySchedulingInfo {
  private QueryId queryId;
  private Integer priority;
  private Long startTime;
  private String assignedQueueName;
  private Set<String> candidateQueueNames;
  private Session session;

  public QuerySchedulingInfo(QueryId queryId, Integer priority, Long startTime, Session session) {
    this.queryId = queryId;
    this.priority = priority;
    this.startTime = startTime;
    this.session = session;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public Integer getPriority() {
    return priority;
  }

  public Long getStartTime() {
    return startTime;
  }

  public String getName() {
    return queryId.getId();
  }

  public String getAssignedQueueName() {
    return assignedQueueName;
  }

  public void setAssignedQueueName(String assignedQueueName) {
    this.assignedQueueName = assignedQueueName;
    session.setVariable(Scheduler.QUERY_QUEUE_KEY, assignedQueueName);
  }

  public Set<String> getCandidateQueueNames() {
    return candidateQueueNames;
  }

  public void setCandidateQueueNames(Set<String> candidateQueueNames) {
    this.candidateQueueNames = candidateQueueNames;
  }

  public Session getSession() {
    return session;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(startTime, getName(), priority);
  }

  @Override
  public boolean equals(Object obj) {
    if ( !(obj instanceof QuerySchedulingInfo)) {
      return false;
    }

    QuerySchedulingInfo other = (QuerySchedulingInfo)obj;
    return queryId.equals(other.queryId);
  }
}
