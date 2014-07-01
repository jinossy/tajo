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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcChannelFactory;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.util.ApplicationIdUtils;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

// life cycle of TaskRunnerContext is a EB
public class TaskRunnerContext {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(TaskRunnerContext.class);

  private TaskRunnerManager manager;
  public AtomicInteger completedTasksNum = new AtomicInteger();
  public AtomicInteger succeededTasksNum = new AtomicInteger();
  public AtomicInteger runningTasksNum = new AtomicInteger();
  public AtomicInteger killedTasksNum = new AtomicInteger();
  public AtomicInteger failedTasksNum = new AtomicInteger();
  public AtomicInteger containerIdSeq = new AtomicInteger();

  private ClientSocketChannelFactory channelFactory;
  // for temporal or intermediate files
  private FileSystem localFS;
  // for input files
  private FileSystem defaultFS;
  private ExecutionBlockId executionBlockId;

  private TajoQueryEngine queryEngine;
  private LocalDirAllocator lDirAllocator;
  private RpcConnectionPool connPool;
  private InetSocketAddress qmMasterAddr;
  private WorkerConnectionInfo queryMaster;
  private TajoConf systemConf;
  // for the doAs block
  private UserGroupInformation taskOwner;

  private Reporter reporter;

  //key is a local absolute path of temporal directories
  private Map<String, ThreadPoolExecutor> fetcherExecutorMap = Maps.newHashMap();
  private AtomicBoolean stop = new AtomicBoolean();

  private final LinkedList<TaskRunnerId> taskRunnerIdPool = Lists.newLinkedList();
  // It keeps all of the query unit attempts while a TaskRunner is running.
  private final ConcurrentMap<QueryUnitAttemptId, Task> tasks = Maps.newConcurrentMap();

  private final ConcurrentMap<TaskRunnerId, TaskRunnerHistory> histories = Maps.newConcurrentMap();

  public TaskRunnerContext(TaskRunnerManager manager, ExecutionBlockId executionBlockId, WorkerConnectionInfo queryMaster) throws IOException {
    this.manager = manager;
    this.executionBlockId = executionBlockId;
    this.connPool = RpcConnectionPool.getPool(manager.getTajoConf());
    this.queryMaster = queryMaster;
    this.systemConf = manager.getTajoConf();

    init();
  }

  public void init() throws IOException {

    LOG.info("Tajo Root Dir: " + systemConf.getVar(TajoConf.ConfVars.ROOT_DIR));
    LOG.info("Worker Local Dir: " + systemConf.getVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR));

    // QueryMaster's address
    this.qmMasterAddr = NetUtils.createSocketAddrForHost(queryMaster.getHost(), queryMaster.getQueryMasterPort());
    LOG.info("QueryMaster Address:" + qmMasterAddr);

    UserGroupInformation.setConfiguration(systemConf);
    // TODO - 'load credential' should be implemented
    // Getting taskOwner
    UserGroupInformation taskOwner = UserGroupInformation.createRemoteUser(systemConf.getVar(TajoConf.ConfVars.USERNAME));
    //taskOwner.addToken(token);

    // initialize MasterWorkerProtocol as an actual task owner.
//      this.client =
//          taskOwner.doAs(new PrivilegedExceptionAction<AsyncRpcClient>() {
//            @Override
//            public AsyncRpcClient run() throws Exception {
//              return new AsyncRpcClient(TajoWorkerProtocol.class, masterAddr);
//            }
//          });
//      this.master = client.getStub();


    // initialize DFS and LocalFileSystems
    this.taskOwner = taskOwner;
    this.defaultFS = TajoConf.getTajoRootDir(systemConf).getFileSystem(systemConf);
    this.localFS = FileSystem.getLocal(systemConf);

    // initialize LocalDirAllocator
    lDirAllocator = new LocalDirAllocator(TajoConf.ConfVars.WORKER_TEMPORAL_DIR.varname);

    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory fetcherFactory = builder.setNameFormat("Fetcher executor #%d").build();

    Iterable<Path> iter = lDirAllocator.getAllLocalPathsToRead(".", systemConf);
    for (Path localDir : iter){
      if(!fetcherExecutorMap.containsKey(localDir)){
        ThreadPoolExecutor fetcherExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(
            systemConf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM), fetcherFactory);
        fetcherExecutorMap.put(localDir.toUri().getRawPath(), fetcherExecutor);
      }

    }
    // Setup QueryEngine according to the query plan
    // Here, we can setup row-based query engine or columnar query engine.
    this.queryEngine = new TajoQueryEngine(systemConf);

    this.reporter = new Reporter();
  }

  public QueryMasterProtocol.QueryMasterProtocolService.Interface getQueryMasterStub() throws Exception {
    NettyClientBase clientBase = null;
    try {
      clientBase = connPool.getConnection(qmMasterAddr, QueryMasterProtocol.class, true);
      return clientBase.getStub();
    } finally {
      connPool.releaseConnection(clientBase);
    }
  }

  public void stop(){
    if(stop.getAndSet(true)){
      return;
    }

    taskRunnerIdPool.clear();

    try {
      reporter.stop();
    } catch (InterruptedException e) {
      LOG.error(e);
    }
    // If ExecutionBlock is stopped, all running or pending tasks will be marked as failed.
    for (Task task : tasks.values()) {
      if (task.getStatus() == TajoProtos.TaskAttemptState.TA_PENDING ||
          task.getStatus() == TajoProtos.TaskAttemptState.TA_RUNNING) {
        task.setState(TajoProtos.TaskAttemptState.TA_FAILED);
        try{
          task.abort();
        } catch (Throwable e){
          LOG.error(e);
        }
      }
    }
    tasks.clear();

    try {
      for(ExecutorService executorService : fetcherExecutorMap.values()){
        executorService.shutdown();
      }
      fetcherExecutorMap.clear();
      releaseShuffleChannelFactory();
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public TajoConf getConf() {
    return manager.getTajoConf();
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

  public Map<QueryUnitAttemptId, Task> getTasks() {
    return tasks;
  }

  public boolean containsTask(QueryUnitAttemptId queryUnitAttemptId) {
    return tasks.containsKey(queryUnitAttemptId);
  }

  public Task getTask(QueryUnitAttemptId taskId) {
    return tasks.get(taskId);
  }

  public void putTask(QueryUnitAttemptId queryUnitAttemptId, Task task) {
    tasks.put(queryUnitAttemptId, task);
  }

  public Task removeTask(QueryUnitAttemptId queryUnitAttemptId) {
    return tasks.remove(queryUnitAttemptId);
  }

  public ExecutorService getFetchLauncher(String outPutPath) {
    // for random access
    ExecutorService fetcherExecutor = null;
    int minScheduledSize = Integer.MAX_VALUE;

    for (Map.Entry<String, ThreadPoolExecutor> entry : fetcherExecutorMap.entrySet()) {
      if (outPutPath.startsWith(entry.getKey())) {
        fetcherExecutor = entry.getValue();
        break;
      }

      int scheduledSize = entry.getValue().getQueue().size();
      if(minScheduledSize > scheduledSize){
        fetcherExecutor = entry.getValue();
        minScheduledSize = scheduledSize;
      }
    }
    return fetcherExecutor;
  }

  // for the local temporal dir
  public Path getBaseDir() throws IOException {
    // the base dir for an output dir
    String baseDir = executionBlockId.getQueryId().toString() + "/output" + "/" + executionBlockId.getId();
    Path baseDirPath = localFS.makeQualified(lDirAllocator.getLocalPathForWrite(baseDir, systemConf));
    LOG.info("TaskRunnerContext basedir is created (" + baseDir +")");
    return baseDirPath;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public void stopTask(TaskRunnerId id){
    manager.stopTask(id);
  }

  public TajoWorker.WorkerContext getWorkerContext(){
    return manager.getWorkerContext();
  }

  public void releaseTaskRunnerId(TaskRunnerId taskRunnerId) {
    taskRunnerIdPool.addLast(taskRunnerId);
  }

  /* Shareable taskRunnerId must be released back to the taskRunnerIdPool when a taskRunner completed. */
  public TaskRunnerId getTaskRunnerId() {
    synchronized (taskRunnerIdPool) {
      if(taskRunnerIdPool.isEmpty()){
        taskRunnerIdPool.addLast(newTaskRunnerId());
      }
      return taskRunnerIdPool.pollFirst();
    }
  }

  public TaskRunnerId newTaskRunnerId() {
    ApplicationAttemptId applicationAttemptId = ApplicationIdUtils.createApplicationAttemptId(executionBlockId);
    return new TaskRunnerId(applicationAttemptId, getWorkerContext().getConnectionInfo().getId() + containerIdSeq.incrementAndGet());
  }

  public void addTaskHistory(TaskRunnerId taskRunnerId, QueryUnitAttemptId quAttemptId, TaskHistory taskHistory) {
    getTaskRunnerHistory(taskRunnerId).addTaskHistory(quAttemptId, taskHistory);
  }

  public TaskRunnerHistory getTaskRunnerHistory(TaskRunnerId taskRunnerId){
    histories.putIfAbsent(taskRunnerId, new TaskRunnerHistory(taskRunnerId, executionBlockId));
    return histories.get(taskRunnerId);
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

  protected class Reporter {
    private Thread reporterThread;
    private AtomicBoolean reporterStop = new AtomicBoolean();
    private static final int PROGRESS_INTERVAL = 3000;
    private static final int MAX_RETRIES = 10;

    public Reporter() {
      this.reporterThread = new Thread(createReporterThread());
      this.reporterThread.setName("Task reporter");
    }

    Runnable createReporterThread() {

      return new Runnable() {
        int remainingRetries = MAX_RETRIES;
        QueryMasterProtocol.QueryMasterProtocolService.Interface masterStub;
        @Override
        public void run() {
          while (!reporterStop.get() && !Thread.interrupted()) {
            try {
              masterStub = getQueryMasterStub();

              if(tasks.size() == 0){
                masterStub.ping(null, getExecutionBlockId().getProto(), NullCallback.get());
              } else {
                for (Task task : new ArrayList<Task>(tasks.values())){
                  task.updateProgress();

                  if(task.isProgressChanged()){
                    masterStub.statusUpdate(null, task.getReport(), NullCallback.get());
                  }
                }
              }
            } catch (Throwable t) {
              LOG.error(t.getMessage(), t);
              remainingRetries -=1;
              if (remainingRetries == 0) {
                ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
                LOG.warn("Last retry, exiting ");
                throw new RuntimeException(t);
              }
            } finally {
              if (remainingRetries > 0 && !reporterStop.get()) {
                synchronized (reporterThread) {
                  try {
                    reporterThread.wait(PROGRESS_INTERVAL);
                  } catch (InterruptedException e) {
                  }
                }
              }
            }
          }
        }
      };
    }

    public void stop() throws InterruptedException {
      if (reporterStop.getAndSet(true)) {
        return;
      }

      if (reporterThread != null) {
        // Intent of the lock is to not send an interupt in the middle of an
        // umbilical.ping or umbilical.statusUpdate
        synchronized (reporterThread) {
          //Interrupt if sleeping. Otherwise wait for the RPC call to return.
          reporterThread.notifyAll();
        }
      }
    }
  }
}
