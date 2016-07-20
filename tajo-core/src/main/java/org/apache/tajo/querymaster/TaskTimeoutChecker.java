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

package org.apache.tajo.querymaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.event.TaskTAttemptFailedEvent;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The driver class for Task timeout processing.
 */
public class TaskTimeoutChecker implements Runnable {
  private static final Log LOG = LogFactory.getLog(TaskTimeoutChecker.class);
  private final QueryMaster.QueryMasterContext context;
  private final Stage stage;
  private int timeoutSec;
  private volatile ScheduledFuture<?> timeoutFuture;


  public TaskTimeoutChecker(QueryMaster.QueryMasterContext context, Stage stage) {
    this.context = context;
    this.stage = stage;
  }

  public void start() {
    this.timeoutSec = context.getConf().getIntVar(TajoConf.ConfVars.QUERYMASTER_TASK_TIMEOUT);
    this.timeoutFuture = this.context.getAsyncTaskService().schedule(this, timeoutSec, TimeUnit.SECONDS);
  }

  public void stop() {
    this.timeoutFuture.cancel(true);
  }

  @Override
  public void run() {
    if(stage.getSynchronizedState() == StageState.RUNNING) {

      for(Task task : stage.getTasks()) {
        if(task.getLastAttemptStatus() == TajoProtos.TaskAttemptState.TA_RUNNING) {
          TaskAttempt attempt = task.getLastAttempt();
          long duration = System.currentTimeMillis() - attempt.getLastContactTime();
          if (TimeUnit.MILLISECONDS.toSeconds(duration) > timeoutSec) {
            //TODO handle to fail for retry
            stage.getEventHandler().handle(new TaskTAttemptFailedEvent(attempt.getId(), null));
            LOG.error("TaskAttempt" + attempt.getId() + " is timed out " + duration + " ms");
          }
        }
      }

      // reschedule this
      this.timeoutFuture = context.getAsyncTaskService().schedule(this, timeoutSec, TimeUnit.SECONDS);
    }
  }
}
