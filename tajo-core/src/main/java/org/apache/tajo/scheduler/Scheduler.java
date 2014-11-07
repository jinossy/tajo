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

import org.apache.tajo.QueryId;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.master.querymaster.QueryJobManager;

public interface Scheduler {
  public static final String QUERY_QUEUE_KEY = "tajo.job.assigned.queue";
  public static final String DEFAULT_QUEUE_NAME = "default";

  public void init(QueryJobManager queryJobManager);

  public void start();

  public void stop();

  public Mode getMode();

  public String getName();

  public int getRunningQueries(String queueName);

  public void addQuery(QueryInProgress resource) throws Exception;

  public void notifyQueryStop(QueryId queryId);

  public String getStatusHtml();

  public enum Mode {
    FIFO, FAIR
  }
}
