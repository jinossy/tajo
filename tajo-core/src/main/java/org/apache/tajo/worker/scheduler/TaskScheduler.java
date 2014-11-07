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
import org.apache.log4j.Logger;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.engine.query.QueryUnitRequest;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A TaskScheduler is a buffer that holds task reservations until an application backend is
 * available to run the task. When a backend is ready, the TaskScheduler requests the task
 * from the {@link Scheduler} that submitted the reservation.
 *
 * Each scheduler will implement a different policy determining when to launch tasks.
 *
 * Schedulers are required to be thread safe, as they will be accessed concurrently from
 * multiple threads.
 */
public abstract class TaskScheduler {
  private final static Logger LOG = Logger.getLogger(TaskScheduler.class);

  protected Configuration conf;
  private final BlockingQueue<QueryUnitRequest> runnableTaskQueue =
      new LinkedBlockingQueue<QueryUnitRequest>();

  /** Initialize the task scheduler, passing it the current available resources
   *  on the machine. */
  void initialize(Configuration conf, int nodeMonitorPort) {
    this.conf = conf;
  }

  /**
   * Get the next task available for launching. This will block until a task is available.
   */
  QueryUnitRequest getNextTask() {
    QueryUnitRequest task = null;
    try {
      task = runnableTaskQueue.take();
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    return task;
  }

  /**
   * Returns the current number of runnable tasks (for testing).
   */
  int runnableTasks() {
    return runnableTaskQueue.size();
  }

  void tasksFinished(List<QueryUnitAttemptId> finishedTasks) {
    for (QueryUnitAttemptId t : finishedTasks) {

      handleTaskFinished(t);
    }
  }

  void noTaskForReservation(QueryUnitRequest taskReservation) {
    handleNoTaskForReservation(taskReservation);
  }

  protected void makeTaskRunnable(QueryUnitRequest task) {
    try {
      LOG.debug("Putting reservation for request " + task.getId() + " in runnable queue");
      runnableTaskQueue.put(task);
    } catch (InterruptedException e) {
      LOG.fatal("Unable to add task to runnable queue: " + e.getMessage());
    }
  }

  public synchronized void submitTaskReservations(List<QueryUnitRequest> request,
                                                  InetSocketAddress appBackendAddress) {
    for (int i = 0; i < request.size(); ++i) {

      int queuedReservations = handleSubmitTaskReservation(request.get(i));
    }
  }

  // TASK SCHEDULERS MUST IMPLEMENT THE FOLLOWING.

  /**
   * Handles a task reservation. Returns the number of queued reservations.
   */
  abstract int handleSubmitTaskReservation(QueryUnitRequest taskReservation);

  /**
   * Cancels all task reservations with the given request id. Returns the number of task
   * reservations cancelled.
   */
  abstract int cancelTaskReservations(QueryUnitAttemptId requestId);

  /**
   * Handles the completion of a task that has finished executing.
   */
  protected abstract void handleTaskFinished(QueryUnitAttemptId taskId);

  /**
   * Handles the case when the node monitor tried to launch a task for a reservation, but
   * the corresponding scheduler didn't return a task (typically because all of the corresponding
   * job's tasks have been launched).
   */
  protected abstract void handleNoTaskForReservation(QueryUnitRequest taskSpec);

  /**
   * Returns the maximum number of active tasks allowed (the number of slots).
   *
   * -1 signals that the scheduler does not enforce a maximum number of active tasks.
   */
  abstract int getMaxActiveTasks();
}
