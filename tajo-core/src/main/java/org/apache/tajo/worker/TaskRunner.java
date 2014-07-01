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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.engine.query.QueryUnitRequestImpl;
import org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NullCallback;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;

/**
 * The driver class for Tajo QueryUnit processing.
 */
public class TaskRunner implements Runnable{

  /** class logger */
  private static final Log LOG = LogFactory.getLog(TaskRunner.class);

  /**
   * TaskRunner states
   */
  public enum STATE {
    /** Constructed but not initialized */
    NOTINITED(0, "NOTINITED"),

    /** Initialized but not started or stopped */
    INITED(1, "INITED"),

    /** inited and not running */
    STARTED(2, "STARTED"),

    /** started and not running */
    PENDING(3, "PENDING"),

    /** started and not pending and not stopped  */
    RUNNING(4, "RUNNING"),

    /** stopped. No further state transitions are permitted */
    STOPPED(5, "STOPPED");

    /**
     * An integer value for use in array lookup and JMX interfaces.
     * Although {@link Enum#ordinal()} could do this, explicitly
     * identify the numbers gives more stability guarantees over time.
     */
    private final int value;

    /**
     * A name of the state that can be used in messages
     */
    private final String statename;

    private STATE(int value, String name) {
      this.value = value;
      this.statename = name;
    }

    /**
     * Get the integer value of a state
     * @return the numeric value of the state
     */
    public int getValue() {
      return value;
    }

    /**
     * Get the name of a state
     * @return the state's name
     */
    @Override
    public String toString() {
      return statename;
    }
  }

  private static final int MAX_GET_TASK_RETRY = 100;
  private final AtomicBoolean stop = new AtomicBoolean();
  private final TaskRunnerId taskRunnerId;

  // Cluster Management
  //private TajoWorkerProtocol.TajoWorkerProtocolService.Interface master;

  // Contains the object references related for TaskRunner
  private TaskRunnerContext taskRunnerContext;

  private long startTime;
  private long finishTime;

  private TaskRunnerHistory history;
  private STATE state;
  private Task task = null;

  public TaskRunner(TaskRunnerContext context,
                    TaskRunnerId taskRunnerId) {
    setState(STATE.NOTINITED);
    this.taskRunnerId = taskRunnerId;
    this.taskRunnerContext = context;
    this.history = context.getTaskRunnerHistory(taskRunnerId);
    this.history.setState(getState());
  }

  public STATE getState(){
    return state;
  }

  public void setState(STATE state){
    this.state = state;
  }

  public TaskRunnerId getId() {
    return taskRunnerId;
  }

  public void init() {
    setState(STATE.INITED);
    this.history.setState(getState());
  }

  public void stop() {
    // If this flag become true, TaskRunner will be terminated.
    if(stop.getAndSet(true)) {
      return;
    }

    if(task != null && task.isRunning()){
      task.abort();
    }
    this.finishTime = System.currentTimeMillis();
    this.history.setFinishTime(finishTime);


    //notify to TaskRunnerManager
    getContext().stopTask(getId());  // TODO remove?
    LOG.info("Stop TaskRunner: " + getContext().getExecutionBlockId());
    setState(STATE.STOPPED);
    this.history.setState(getState());
  }

  public long getStartTime() {
    return startTime;
  }
  public long getFinishTime() {
    return finishTime;
  }

  public TaskRunnerContext getContext() {
    return taskRunnerContext;
  }

  static void fatalError(QueryMasterProtocolService.Interface qmClientService,
                         QueryUnitAttemptId taskAttemptId, String message) {
    if (message == null) {
       message = "No error message";
    }
    TaskFatalErrorReport.Builder builder = TaskFatalErrorReport.newBuilder()
        .setId(taskAttemptId.getProto())
        .setErrorMessage(message);

    qmClientService.fatalError(null, builder.build(), NullCallback.get());
  }

  /*A thread to receive each assigned query unit and execute the query unit*/
  @Override
  public void run() {
    setState(STATE.STARTED);
    getContext().runningTasksNum.incrementAndGet();
    startTime = System.currentTimeMillis();
    this.history.setStartTime(startTime);
    this.history.setState(getState());
    LOG.info("TaskRunner state:" + getState());

    CallFuture<QueryUnitRequestProto> callFuture = null;
    QueryUnitRequestProto taskRequest = null;
    QueryMasterProtocolService.Interface qmClientService;
    try {
      qmClientService = getContext().getQueryMasterStub();
      setState(STATE.PENDING);

      int retryCount = 0;
      while (!stop.get() && !Thread.interrupted()) {
        if (callFuture == null) {
          callFuture = new CallFuture<QueryUnitRequestProto>();
          LOG.info("Request GetTask: " + getId());
          GetTaskRequestProto request = GetTaskRequestProto.newBuilder()
              .setExecutionBlockId(getContext().getExecutionBlockId().getProto())
              .setContainerId(taskRunnerId.getProto())
              .setWorkerId(getContext().getWorkerContext().getConnectionInfo().getId())
              .build();

          qmClientService.getTask(null, request, callFuture);
        }
        try {
          // wait for an assigning task for 3 seconds
          taskRequest = callFuture.get(3, TimeUnit.SECONDS);
          break;
        } catch (InterruptedException e) {
          if (isStopped()) {
            break;
          }
        } catch (TimeoutException te) {
          if (isStopped()) {
            break;
          }

          if(retryCount > MAX_GET_TASK_RETRY){
            // yield to other taskrunner. most case, a netty stub has been disconnect
            break;
          }
          // if there has been no assigning task for a given period,
          // TaskRunner will retry to request an assigning task.
          retryCount++;
          LOG.info("Retry(" + retryCount + ") assigning task:" + getId() + " state:" + getState());
          continue;
        }
      }


      if (!isStopped() && taskRequest != null) {
        // QueryMaster can send the terminal signal to TaskRunner.
        // If TaskRunner receives the terminal signal, TaskRunner will be terminated
        // immediately.
        if (taskRequest.getShouldDie()) {
          LOG.info("Received ShouldDie flag:" + getId());
          stop();
        } else {
          getContext().getWorkerContext().getWorkerSystemMetrics().counter("query", "task").inc();

          QueryUnitAttemptId taskAttemptId = new QueryUnitAttemptId(taskRequest.getId());
          if (getContext().containsTask(taskAttemptId)) {
            LOG.error("Duplicate Task Attempt: " + taskAttemptId);
            fatalError(qmClientService, taskAttemptId, "Duplicate Task Attempt: " + taskAttemptId);
          } else {
            LOG.info("Initializing: " + taskAttemptId);
            setState(STATE.RUNNING);
            try {
              task = new Task(taskRunnerId, taskRunnerContext, qmClientService,
                  new QueryUnitRequestImpl(taskRequest));
              getContext().putTask(taskAttemptId, task);

              task.init();
              if (task.hasFetchPhase()) {
                task.fetch(); // The fetch is performed in an asynchronous way.
              }
              // task.run() is a blocking call.
              task.run();
            } catch (Throwable t) {
              LOG.error(t.getMessage(), t);
              fatalError(qmClientService, taskAttemptId, t.getMessage());
            } finally {
              if (task != null && task.getStatus() != TaskAttemptState.TA_SUCCEEDED) {
                task.abort();
              } else {
                LOG.info("complete task runner:" + getId());
              }
            }
          }
        }
      }

    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. TaskRunner shutting down.", t);
    } finally {
      stop();
      getContext().runningTasksNum.decrementAndGet();
      getContext().releaseTaskRunnerId(taskRunnerId);
    }
  }

  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopped() {
    return this.stop.get();
  }

  public TaskRunnerHistory getHistory() {
    return history;
  }
}
