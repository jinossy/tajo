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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.physical.PhysicalExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.engine.query.QueryUnitRequest;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService;
import org.apache.tajo.ipc.TajoWorkerProtocol.*;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.fragment.FileFragment;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class Task {
  private static final Log LOG = LogFactory.getLog(Task.class);
  private static final float FETCHER_PROGRESS = 0.5f;

  private final QueryContext queryContext;
  private TaskRunnerContext taskRunnerContext;
  private final QueryUnitAttemptId taskId;
  private final TaskRunnerId taskRunnerId;

  private final Path taskDir;
  private final QueryUnitRequest request;
  private TaskAttemptContext context;
  private List<Fetcher> fetcherRunners;
  private LogicalNode plan;
  private final Map<String, TableDesc> descs = Maps.newHashMap();
  private PhysicalExec executor;
  private boolean interQuery;
  private boolean killed = false;
  private boolean aborted = false;
  private boolean stopped = false;
  private Path inputTableBaseDir;

  private long startTime;
  private long finishTime;

  private final TableStats inputStats;

  // TODO - to be refactored
  private ShuffleType shuffleType = null;
  private Schema finalSchema = null;
  private TupleComparator sortComp = null;

  static final String OUTPUT_FILE_PREFIX="part-";
  static final ThreadLocal<NumberFormat> OUTPUT_FILE_FORMAT_SUBQUERY =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };
  static final ThreadLocal<NumberFormat> OUTPUT_FILE_FORMAT_TASK =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };

  public Task(final TaskRunnerId taskRunnerId,
              final TaskRunnerContext worker,
              final QueryMasterProtocolService.Interface masterProxy,
              final QueryUnitRequest request) throws IOException {
    this.request = request;
    this.taskId = request.getId();
    this.taskRunnerId = taskRunnerId;

    this.queryContext = request.getQueryContext();
    this.taskRunnerContext = worker;
    this.taskDir = StorageUtil.concatPath(taskRunnerContext.getBaseDir(),
        taskId.getQueryUnitId().getId() + "_" + taskId.getId());

    this.context = new TaskAttemptContext(taskRunnerContext.getConf(), queryContext, taskId,
        request.getFragments().toArray(new FragmentProto[request.getFragments().size()]), taskDir);
    this.context.setDataChannel(request.getDataChannel());
    this.context.setEnforcer(request.getEnforcer());
    this.inputStats = new TableStats();

    plan = CoreGsonHelper.fromJson(request.getSerializedData(), LogicalNode.class);
    LogicalNode [] scanNode = PlannerUtil.findAllNodes(plan, NodeType.SCAN);
    if (scanNode != null) {
      for (LogicalNode node : scanNode) {
        ScanNode scan = (ScanNode) node;
        descs.put(scan.getCanonicalName(), scan.getTableDesc());
      }
    }

    LogicalNode [] partitionScanNode = PlannerUtil.findAllNodes(plan, NodeType.PARTITIONS_SCAN);
    if (partitionScanNode != null) {
      for (LogicalNode node : partitionScanNode) {
        PartitionedTableScanNode scan = (PartitionedTableScanNode) node;
        descs.put(scan.getCanonicalName(), scan.getTableDesc());
      }
    }

    interQuery = request.getProto().getInterQuery();
    if (interQuery) {
      context.setInterQuery();
      this.shuffleType = context.getDataChannel().getShuffleType();

      if (shuffleType == ShuffleType.RANGE_SHUFFLE) {
        SortNode sortNode = PlannerUtil.findTopNode(plan, NodeType.SORT);
        this.finalSchema = PlannerUtil.sortSpecsToSchema(sortNode.getSortKeys());
        this.sortComp = new TupleComparator(finalSchema, sortNode.getSortKeys());
      }
    } else {
      // The final result of a task will be written in a file named part-ss-nnnnnnn,
      // where ss is the subquery id associated with this task, and nnnnnn is the task id.
      Path outFilePath = StorageUtil.concatPath(queryContext.getStagingDir(), TajoConstants.RESULT_DIR_NAME,
          OUTPUT_FILE_PREFIX +
          OUTPUT_FILE_FORMAT_SUBQUERY.get().format(taskId.getQueryUnitId().getExecutionBlockId().getId()) + "-" +
          OUTPUT_FILE_FORMAT_TASK.get().format(taskId.getQueryUnitId().getId()));
      LOG.info("Output File Path: " + outFilePath);
      context.setOutputPath(outFilePath);
    }

    context.setState(TaskAttemptState.TA_PENDING);
    LOG.info("==================================");
    LOG.info("* Subquery " + request.getId() + " is initialized");
    LOG.info("* InterQuery: " + interQuery
        + (interQuery ? ", Use " + this.shuffleType + " shuffle":""));

    LOG.info("* Fragments (num: " + request.getFragments().size() + ")");
    LOG.info("* Fetches (total:" + request.getFetches().size() + ") :");
    for (FetchImpl f : request.getFetches()) {
      LOG.info("Table Id: " + f.getName() + ", Simple URIs: " + f.getSimpleURIs());
    }
    LOG.info("* Local task dir: " + taskDir);
    if(LOG.isDebugEnabled()) {
      LOG.debug("* plan:\n");
      LOG.debug(plan.toString());
    }
    LOG.info("==================================");
  }

  public void init() throws IOException {
    // initialize a task temporal dir
    FileSystem localFS = taskRunnerContext.getLocalFS();
    localFS.mkdirs(taskDir);

    if (request.getFetches().size() > 0) {
      inputTableBaseDir = localFS.makeQualified(
          taskRunnerContext.getLocalDirAllocator().getLocalPathForWrite(
              getTaskAttemptDir(context.getTaskId()).toString(), taskRunnerContext.getConf())
      );
      localFS.mkdirs(inputTableBaseDir);
      Path tableDir;
      for (String inputTable : context.getInputTables()) {
        tableDir = new Path(inputTableBaseDir, inputTable);
        if (!localFS.exists(tableDir)) {
          LOG.info("the directory is created  " + tableDir.toUri());
          localFS.mkdirs(tableDir);
        }
      }
    }
    // for localizing the intermediate data
    localize(request);
  }

  public QueryUnitAttemptId getTaskId() {
    return taskId;
  }

  public static Log getLog() {
    return LOG;
  }

  public void localize(QueryUnitRequest request) throws IOException {
    fetcherRunners = getFetchRunners(context, request.getFetches());
  }

  public QueryUnitAttemptId getId() {
    return context.getTaskId();
  }

  public TaskAttemptState getStatus() {
    return context.getState();
  }

  public String toString() {
    return "queryId: " + this.getId() + " status: " + this.getStatus();
  }

  public void setState(TaskAttemptState status) {
    context.setState(status);
  }

  public TaskAttemptContext getContext() {
    return context;
  }

  public boolean hasFetchPhase() {
    return fetcherRunners.size() > 0;
  }

  public List<Fetcher> getFetchers() {
    return new ArrayList<Fetcher>(fetcherRunners);
  }

  public void fetch() {
    for (Fetcher f : fetcherRunners) {
      ExecutorService executor  = taskRunnerContext.getFetchLauncher(f.getOutputPath());
      executor.submit(new FetchRunner(context, f));
    }
  }

  public void kill() {
    killed = true;
    context.stop();
    context.setState(TaskAttemptState.TA_KILLED);
  }

  public void abort() {
    aborted = true;
    context.stop();
  }

  public void cleanUp() {
    // remove itself from worker
    if (context.getState() == TaskAttemptState.TA_SUCCEEDED) {
      try {
        taskRunnerContext.getLocalFS().delete(context.getWorkDir(), true);
        synchronized (taskRunnerContext.getTasks()) {
          taskRunnerContext.getTasks().remove(this.getId());
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    } else {
      LOG.error("QueryUnitAttemptId: " + context.getTaskId() + " status: " + context.getState());
    }
  }

  public TaskStatusProto getReport() {
    TaskStatusProto.Builder builder = TaskStatusProto.newBuilder();
    builder.setWorkerName(taskRunnerContext.getWorkerContext().getConnectionInfo().getHostAndPeerRpcPort());
    builder.setId(context.getTaskId().getProto())
        .setProgress(context.getProgress())
        .setState(context.getState());

    builder.setInputStats(reloadInputStats());

    if (context.getResultStats() != null) {
      builder.setResultStats(context.getResultStats().getProto());
    }
    return builder.build();
  }

  public boolean isRunning(){
    return context.getState() == TaskAttemptState.TA_RUNNING;
  }
  public boolean isProgressChanged() {
    return context.isProgressChanged();
  }

  public void updateProgress() {
    if(killed || aborted){
      return;
    }

    if (executor != null && context.getProgress() < 1.0f) {
      float progress = executor.getProgress();
      context.setExecutorProgress(progress);
    }
  }

  private CatalogProtos.TableStatsProto reloadInputStats() {
    synchronized(inputStats) {
      if (this.executor == null) {
        return inputStats.getProto();
      }

      TableStats executorInputStats = this.executor.getInputStats();

      if (executorInputStats != null) {
        inputStats.setValues(executorInputStats);
      }
      return inputStats.getProto();
    }
  }

  private TaskCompletionReport getTaskCompletionReport() {
    TaskCompletionReport.Builder builder = TaskCompletionReport.newBuilder();
    builder.setId(context.getTaskId().getProto());
    builder.setContainerId(taskRunnerId.getProto());
    builder.setInputStats(reloadInputStats());

    if (context.hasResultStats()) {
      builder.setResultStats(context.getResultStats().getProto());
    } else {
      builder.setResultStats(new TableStats().getProto());
    }

    Iterator<Entry<Integer,String>> it = context.getShuffleFileOutputs();
    if (it.hasNext()) {
      do {
        Entry<Integer,String> entry = it.next();
        ShuffleFileOutput.Builder part = ShuffleFileOutput.newBuilder();
        part.setPartId(entry.getKey());
        builder.addShuffleFileOutputs(part.build());
      } while (it.hasNext());
    }

    return builder.build();
  }

  private void waitForFetch() throws InterruptedException, IOException {
    context.getFetchLatch().await();
    LOG.info(context.getTaskId() + " All fetches are done!");
    Collection<String> inputs = Lists.newArrayList(context.getInputTables());
    for (String inputTable: inputs) {
      File tableDir = new File(context.getFetchIn(), inputTable);
      FileFragment[] frags = localizeFetchedData(tableDir, inputTable, descs.get(inputTable).getMeta());
      context.updateAssignedFragments(inputTable, frags);
    }
  }

  public void run() throws Exception {
    startTime = System.currentTimeMillis();
    Exception error = null;
    try {
      context.setState(TaskAttemptState.TA_RUNNING);

      if (context.hasFetchPhase()) {
        // If the fetch is still in progress, the query unit must wait for
        // complete.
        waitForFetch();
        context.setFetcherProgress(FETCHER_PROGRESS);
        context.setProgress(FETCHER_PROGRESS);
      }

      this.executor = taskRunnerContext.getTQueryEngine().
          createPlan(context, plan);
      this.executor.init();

      while(!killed && !aborted && executor.next() != null) {
      }
      this.executor.close();
      reloadInputStats();
      this.executor = null;
    } catch (Exception e) {
      error = e ;
      LOG.error(e.getMessage(), e);
      aborted = true;
    } finally {
      stopped = true;
      taskRunnerContext.completedTasksNum.incrementAndGet();
      QueryMasterProtocol.QueryMasterProtocolService.Interface queryMasterStub =  taskRunnerContext.getQueryMasterStub();
      if (killed || aborted) {
        context.setExecutorProgress(0.0f);
        context.setProgress(0.0f);
        if(killed) {
          context.setState(TaskAttemptState.TA_KILLED);
          queryMasterStub.statusUpdate(null, getReport(), NullCallback.get());
          taskRunnerContext.killedTasksNum.incrementAndGet();
        } else {
          context.setState(TaskAttemptState.TA_FAILED);
          TaskFatalErrorReport.Builder errorBuilder =
              TaskFatalErrorReport.newBuilder()
                  .setId(getId().getProto());
          if (error != null) {
            if (error.getMessage() == null) {
              errorBuilder.setErrorMessage(error.getClass().getCanonicalName());
            } else {
              errorBuilder.setErrorMessage(error.getMessage());
            }
            errorBuilder.setErrorTrace(ExceptionUtils.getStackTrace(error));
          }

          queryMasterStub.fatalError(null, errorBuilder.build(), NullCallback.get());
          taskRunnerContext.failedTasksNum.incrementAndGet();
        }
      } else {
        // if successful
        context.setProgress(1.0f);
        context.setState(TaskAttemptState.TA_SUCCEEDED);
        taskRunnerContext.succeededTasksNum.incrementAndGet();

        TaskCompletionReport report = getTaskCompletionReport();
        queryMasterStub.done(null, report, NullCallback.get());
      }

      finishTime = System.currentTimeMillis();
      LOG.info("Worker's task counter - total:" + taskRunnerContext.completedTasksNum.intValue() +
          ", succeeded: " + taskRunnerContext.succeededTasksNum.intValue()
          + ", killed: " + taskRunnerContext.killedTasksNum.intValue()
          + ", failed: " + taskRunnerContext.failedTasksNum.intValue());
      cleanupTask();
    }
  }

  public void cleanupTask() {
    taskRunnerContext.addTaskHistory(taskRunnerId, getId(), createTaskHistory());
    taskRunnerContext.removeTask(getId());
    taskRunnerContext = null;

    fetcherRunners.clear();
    fetcherRunners = null;
    try {
      if(executor != null) {
        executor.close();
        executor = null;
      }
    } catch (IOException e) {
      LOG.fatal(e.getMessage(), e);
    }
  }

  public TaskHistory createTaskHistory() {
    TaskHistory taskHistory = null;
    try {
      taskHistory = new TaskHistory(getTaskId(), getStatus(), context.getProgress(),
          startTime, finishTime, reloadInputStats());

      if (context.getOutputPath() != null) {
        taskHistory.setOutputPath(context.getOutputPath().toString());
      }

      if (context.getWorkDir() != null) {
        taskHistory.setWorkingPath(context.getWorkDir().toString());
      }

      if (context.getResultStats() != null) {
        taskHistory.setOutputStats(context.getResultStats().getProto());
      }

      if (hasFetchPhase()) {
        taskHistory.setTotalFetchCount(fetcherRunners.size());
        int i = 0;
        FetcherHistoryProto.Builder builder = FetcherHistoryProto.newBuilder();
        for (Fetcher fetcher : fetcherRunners) {
          // TODO store the fetcher histories
          if (taskRunnerContext.getConf().getBoolVar(TajoConf.ConfVars.TAJO_DEBUG)) {
            builder.setStartTime(fetcher.getStartTime());
            builder.setFinishTime(fetcher.getFinishTime());
            builder.setFileLength(fetcher.getFileLen());
            builder.setMessageReceivedCount(fetcher.getMessageReceiveCount());
            builder.setState(fetcher.getState());

            taskHistory.addFetcherHistory(builder.build());
          }
          if (fetcher.getState() == TajoProtos.FetcherState.FETCH_FINISHED) i++;
        }
        taskHistory.setFinishedFetchCount(i);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    return taskHistory;
  }

  public int hashCode() {
    return context.hashCode();
  }

  public boolean equals(Object obj) {
    if (obj instanceof Task) {
      Task other = (Task) obj;
      return this.context.equals(other.context);
    }
    return false;
  }

  private FileFragment[] localizeFetchedData(File file, String name, TableMeta meta)
      throws IOException {
    Configuration c = new Configuration(taskRunnerContext.getConf());
    c.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "file:///");
    FileSystem fs = FileSystem.get(c);
    Path tablePath = new Path(file.getAbsolutePath());

    List<FileFragment> listTablets = new ArrayList<FileFragment>();
    FileFragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus f : fileLists) {
      if (f.getLen() == 0) {
        continue;
      }
      tablet = new FileFragment(name, f.getPath(), 0l, f.getLen());
      listTablets.add(tablet);
    }

    FileFragment[] tablets = new FileFragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }

  private class FetchRunner implements Runnable {
    private final TaskAttemptContext ctx;
    private final Fetcher fetcher;

    public FetchRunner(TaskAttemptContext ctx, Fetcher fetcher) {
      this.ctx = ctx;
      this.fetcher = fetcher;
    }

    @Override
    public void run() {
      int retryNum = 0;
      int maxRetryNum = 5;
      int retryWaitTime = 1000;

      try { // for releasing fetch latch
        while(!killed && retryNum < maxRetryNum) {
          if (retryNum > 0) {
            try {
              Thread.sleep(retryWaitTime);
            } catch (InterruptedException e) {
              LOG.error(e);
            }
            LOG.warn("Retry on the fetch: " + fetcher.getURI() + " (" + retryNum + ")");
          }
          try {
            File fetched = fetcher.get();
            if (fetched != null) {
              break;
            }
          } catch (IOException e) {
            LOG.error("Fetch failed: " + fetcher.getURI(), e);
          }
          retryNum++;
        }
      } finally {
        fetcherFinished(ctx);
      }

      if (retryNum == maxRetryNum) {
        LOG.error("ERROR: the maximum retry (" + retryNum + ") on the fetch exceeded (" + fetcher.getURI() + ")");
      }
    }
  }

  @VisibleForTesting
  public static float adjustFetchProcess(int totalFetcher, int remainFetcher) {
    return ((float)(totalFetcher - remainFetcher)) / (float)totalFetcher * FETCHER_PROGRESS;
  }

  private synchronized void fetcherFinished(TaskAttemptContext ctx) {
    int fetcherSize = fetcherRunners.size();
    if(fetcherSize == 0) {
      return;
    }

    ctx.getFetchLatch().countDown();

    int remainFetcher = (int)(ctx.getFetchLatch().getCount());
    if (remainFetcher == 0) {
      context.setProgress(FETCHER_PROGRESS);
    } else {
      context.setProgress(adjustFetchProcess(fetcherSize, remainFetcher));
    }
  }

  private List<Fetcher> getFetchRunners(TaskAttemptContext ctx,
                                        List<FetchImpl> fetches) throws IOException {

    if (fetches.size() > 0) {
      ClientSocketChannelFactory channelFactory = taskRunnerContext.getShuffleChannelFactory();
      Path inputDir = taskRunnerContext.getLocalDirAllocator().
          getLocalPathToRead(getTaskAttemptDir(ctx.getTaskId()).toString(), taskRunnerContext.getConf());
      File storeDir;

      int i = 0;
      File storeFile;
      List<Fetcher> runnerList = Lists.newArrayList();
      for (FetchImpl f : fetches) {
        for (URI uri : f.getURIs()) {
          storeDir = new File(inputDir.toString(), f.getName());
          if (!storeDir.exists()) {
            if(!storeDir.mkdirs()){
              throw new IOException("Can't create dir :" + storeDir);
            }
          }
          storeFile = new File(storeDir, "in_" + i);
          Fetcher fetcher = new Fetcher(uri, storeFile, channelFactory);
          runnerList.add(fetcher);
          i++;
        }
      }
      ctx.addFetchPhase(runnerList.size(), new File(inputDir.toString()));
      return runnerList;
    } else {
      return Lists.newArrayList();
    }
  }

  public static Path getTaskAttemptDir(QueryUnitAttemptId quid) {
    Path workDir =
        StorageUtil.concatPath(
            quid.getQueryUnitId().getExecutionBlockId().getQueryId().toString(),
            "in",
            quid.getQueryUnitId().getExecutionBlockId().toString(),
            String.valueOf(quid.getQueryUnitId().getId()),
            String.valueOf(quid.getId()));
    return workDir;
  }
}
