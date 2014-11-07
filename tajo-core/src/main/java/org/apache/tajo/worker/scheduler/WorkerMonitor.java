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

package org.apache.tajo.worker.scheduler;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.engine.query.QueryUnitRequest;

import java.net.UnknownHostException;
import java.util.List;

/**
 * A Worker Monitor which is responsible for communicating with application
 * backends. This class is wrapped by multiple thrift servers, so it may
 * be concurrently accessed when handling multiple function calls
 * simultaneously.
 */
public class WorkerMonitor {
  static final Log LOG = LogFactory.getLog(WorkerMonitor.class);

  public void initialize(Configuration conf, int nodeMonitorInternalPort)
      throws UnknownHostException {
  }



  /**
   * Account for tasks which have finished.
   */
  public void tasksFinished(List<QueryUnitAttemptId> tasks) {
  }

  public boolean enqueueTaskReservations(QueryUnitRequest request) {
    return true;
  }

  public void cancelTaskReservations(QueryUnitAttemptId requestId) {
  }
}
