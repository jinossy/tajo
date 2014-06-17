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

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.rm.TajoWorkerContainerId;
import org.apache.tajo.rpc.RpcChannelFactory;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

// life cycle of TaskRunnerContext is a EB
public class TaskRunnerContext {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(TaskRunnerContext.class);

  private TaskRunnerManager manager;
  public AtomicInteger completedTasksNum = new AtomicInteger();
  public AtomicInteger succeededTasksNum = new AtomicInteger();
  public AtomicInteger killedTasksNum = new AtomicInteger();
  public AtomicInteger failedTasksNum = new AtomicInteger();
  private ClientSocketChannelFactory channelFactory;
  private FileSystem localFS;
  private FileSystem defaultFS;
  private ExecutionBlockId executionBlockId;
  private Path baseDirPath;
  private TajoQueryEngine queryEngine;
  private LocalDirAllocator lDirAllocator;
  private Map<TajoWorkerContainerId, TaskRunnerHistory> histories = Maps.newConcurrentMap();

  public TaskRunnerContext(TaskRunnerManager manager, ExecutionBlockId executionBlockId) {
    this.manager = manager;
    this.executionBlockId = executionBlockId;

    init(manager.getTajoConf());
  }

  public void init(TajoConf systemConf) {

    try {
      // initialize DFS and LocalFileSystems
      this.defaultFS = TajoConf.getTajoRootDir(systemConf).getFileSystem(systemConf);
      this.localFS = FileSystem.getLocal(systemConf);

      // the base dir for an output dir
      String baseDir = executionBlockId.getQueryId().toString() + "/output" + "/" + executionBlockId.getId();

      // initialize LocalDirAllocator
      LocalDirAllocator lDirAllocator = new LocalDirAllocator(TajoConf.ConfVars.WORKER_TEMPORAL_DIR.varname);

      baseDirPath = localFS.makeQualified(lDirAllocator.getLocalPathForWrite(baseDir, systemConf));
      LOG.info("TaskRunnerContext basedir is created (" + baseDir +")");

      // Setup QueryEngine according to the query plan
      // Here, we can setup row-based query engine or columnar query engine.
      this.queryEngine = new TajoQueryEngine(systemConf);
    } catch (Throwable t) {
      t.printStackTrace();
      LOG.error(t);
    }
  }

  public TajoConf getConf() {
    return manager.getTajoConf();
  }

  public NodeId getNodeId() {
    return manager.getWorkerContext().getNodeId();
  }

  public FileSystem getLocalFS() {
    return localFS;
  }

  public FileSystem getDefaultFS() {
    return defaultFS;
  }

  public LocalDirAllocator getLocalDirAllocator() {
    return lDirAllocator;
  }

  public TajoQueryEngine getTQueryEngine() {
    return queryEngine;
  }

//  public Map<QueryUnitAttemptId, Task> getTasks() {
//    return taskRunner.tasks;
//  }

//  public Task getTask(QueryUnitAttemptId taskId) {
//    return taskRunner.tasks.get(taskId);
//  }

  public ExecutorService getFetchLauncher() {
    return manager.getFetcherExecutor();
  }

  public Path getBaseDir() {
    return baseDirPath;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

//  public TajoWorkerContainerId getContainerId() {
//    return taskRunner.containerId;
//  }

  public void stopTask(String id){
    manager.stopTask(id);
  }

  public TajoWorker.WorkerContext getWorkerContext(){
    return manager.getWorkerContext();
  }

  public void addTaskHistory(TajoWorkerContainerId containerId, QueryUnitAttemptId quAttemptId, TaskHistory taskHistory) {
    histories.get(containerId).addTaskHistory(quAttemptId, taskHistory);
  }

  public TaskRunnerHistory getExcutionBlockHistory(TajoWorkerContainerId containerId){
    return histories.get(containerId);
  }

  protected ClientSocketChannelFactory getShuffleChannelFactory(){
    if(channelFactory == null) {
      int workerNum = getConf().getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM);
      channelFactory = RpcChannelFactory.createClientChannelFactory("Fetcher", workerNum);
    }
    return channelFactory;
  }

  protected void releaseShuffleChannelFactory(){
    if(channelFactory != null) {
      channelFactory.shutdown();
      channelFactory.releaseExternalResources();
      channelFactory = null;
    }
  }
}
