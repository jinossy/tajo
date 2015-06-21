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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.JsonHelper;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.event.*;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.LogicalRootNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.verifier.VerifyException;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.rpc.AsyncRpcClient;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.session.Session;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.TableSpaceManager;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.metrics.TajoMetrics;
import org.apache.tajo.util.metrics.reporter.MetricsConsoleReporter;
import org.apache.tajo.worker.event.NodeResourceDeallocateEvent;
import org.apache.tajo.worker.event.NodeResourceEvent;
import org.apache.tajo.worker.event.NodeStatusEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.tajo.TajoProtos.QueryState;

public class QueryMasterTask extends CompositeService {
  private static final Log LOG = LogFactory.getLog(QueryMasterTask.class.getName());

  // query submission directory is private!
  final public static FsPermission STAGING_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx--------

  public static final String TMP_STAGING_DIR_PREFIX = ".staging";

  private QueryId queryId;

  private Session session;

  private QueryContext queryContext;

  private QueryMasterTaskContext queryTaskContext;

  private QueryMaster.QueryMasterContext queryMasterContext;

  private Query query;

  private String jsonExpr;

  private AsyncDispatcher dispatcher;

  private final long querySubmitTime;

  private Map<String, TableDesc> tableDescMap = new HashMap<String, TableDesc>();

  private TajoConf systemConf;

  private AtomicLong lastClientHeartbeat = new AtomicLong(-1);

  private volatile boolean isStopped;

  private TajoMetrics queryMetrics;

  private Throwable initError;

  private NodeResource allocation;

  private final List<TajoWorkerProtocol.TaskFatalErrorReport> diagnostics =
      new ArrayList<TajoWorkerProtocol.TaskFatalErrorReport>();

  private final ConcurrentMap<Integer, WorkerConnectionInfo> workerMap = Maps.newConcurrentMap();

  public QueryMasterTask(QueryMaster.QueryMasterContext queryMasterContext,
                         QueryId queryId, Session session, QueryContext queryContext,
                         String jsonExpr, NodeResource allocation, AsyncDispatcher dispatcher) {

    super(QueryMasterTask.class.getName());
    this.queryMasterContext = queryMasterContext;
    this.queryId = queryId;
    this.session = session;
    this.queryContext = queryContext;
    this.jsonExpr = jsonExpr;
    this.allocation = allocation;
    this.querySubmitTime = System.currentTimeMillis();
    this.dispatcher = dispatcher;
  }

  public QueryMasterTask(QueryMaster.QueryMasterContext queryMasterContext,
                         QueryId queryId, Session session, QueryContext queryContext,
                         String jsonExpr,
                         NodeResource allocation) {
    this(queryMasterContext, queryId, session, queryContext, jsonExpr, allocation, new AsyncDispatcher());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);

    queryTaskContext = new QueryMasterTaskContext();

    addService(dispatcher);

    dispatcher.register(StageEventType.class, new StageEventDispatcher());
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class, new TaskAttemptEventDispatcher());
    dispatcher.register(QueryMasterQueryCompletedEvent.EventType.class, new QueryFinishEventHandler());
    dispatcher.register(TaskSchedulerEvent.EventType.class, new TaskSchedulerDispatcher());
    dispatcher.register(LocalTaskEventType.class, new LocalTaskEventHandler());

    try {
      initStagingDir();
      queryMetrics = new TajoMetrics(queryId.toString());
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      initError = t;
    }
    super.serviceInit(systemConf);
  }

  public boolean isStopped() {
    return isStopped;
  }

  @Override
  public void serviceStart() throws Exception {
    startQuery();
    List<TajoProtos.WorkerConnectionInfoProto> workersProto = queryMasterContext.getQueryMaster().getAllWorker();
    for (TajoProtos.WorkerConnectionInfoProto worker : workersProto) {
      workerMap.put(worker.getId(), new WorkerConnectionInfo(worker));
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    isStopped = true;

    LOG.info("Stopping QueryMasterTask:" + queryId);

    //release QM resource
    EventHandler handler = getQueryTaskContext().getQueryMasterContext().getWorkerContext().
        getNodeResourceManager().getDispatcher().getEventHandler();

    handler.handle(new NodeResourceDeallocateEvent(allocation, NodeResourceEvent.ResourceType.QUERY_MASTER));

    //flush current node resource
    handler.handle(new NodeStatusEvent(NodeStatusEvent.EventType.FLUSH_REPORTS));

    if (!queryContext.getBool(SessionVars.DEBUG_ENABLED)) {
      cleanupQuery(getQueryId());
    }

    if (queryMetrics != null) {
      queryMetrics.report(new MetricsConsoleReporter());
    }

    super.serviceStop();
    LOG.info("Stopped QueryMasterTask:" + queryId);
  }
  //FIXME remove
  public void handleTaskRequestEvent(TaskRequestEvent event) {
    ExecutionBlockId id = event.getExecutionBlockId();
    query.getStage(id).handleTaskRequestEvent(event);
  }

  public void handleTaskFailed(TajoWorkerProtocol.TaskFatalErrorReport report) {
    synchronized(diagnostics) {
      if (diagnostics.size() < 10) {
        diagnostics.add(report);
      }
    }

    getEventHandler().handle(new TaskFatalErrorEvent(report));
  }

  public Collection<TajoWorkerProtocol.TaskFatalErrorReport> getDiagnostics() {
    synchronized(diagnostics) {
      return Collections.unmodifiableCollection(diagnostics);
    }
  }

  private class StageEventDispatcher implements EventHandler<StageEvent> {
    public void handle(StageEvent event) {
      ExecutionBlockId id = event.getStageId();
      if(LOG.isDebugEnabled()) {
        LOG.debug("StageEventDispatcher:" + id + "," + event.getType());
      }
      query.getStage(id).handle(event);
    }
  }

  private class TaskEventDispatcher
      implements EventHandler<TaskEvent> {
    public void handle(TaskEvent event) {
      TaskId taskId = event.getTaskId();
      if(LOG.isDebugEnabled()) {
        LOG.debug("TaskEventDispatcher>" + taskId + "," + event.getType());
      }
      Task task = query.getStage(taskId.getExecutionBlockId()).
          getTask(taskId);
      task.handle(event);
    }
  }

  private class TaskAttemptEventDispatcher
      implements EventHandler<TaskAttemptEvent> {
    public void handle(TaskAttemptEvent event) {
      TaskAttemptId attemptId = event.getTaskAttemptId();
      Stage stage = query.getStage(attemptId.getTaskId().getExecutionBlockId());
      Task task = stage.getTask(attemptId.getTaskId());
      TaskAttempt attempt = task.getAttempt(attemptId);
      attempt.handle(event);
    }
  }

  private class TaskSchedulerDispatcher
      implements EventHandler<TaskSchedulerEvent> {
    public void handle(TaskSchedulerEvent event) {
      Stage stage = query.getStage(event.getExecutionBlockId());
      stage.getTaskScheduler().handle(event);
    }
  }

  /**
   * It sends a kill RPC request to a corresponding worker.
   *
   * @param workerId worker unique Id.
   * @param taskAttemptId The TaskAttemptId to be killed.
   */
  protected void killTaskAttempt(int workerId, TaskAttemptId taskAttemptId) {
    NettyClientBase tajoWorkerRpc;
    ExecutionBlockId ebId = taskAttemptId.getTaskId().getExecutionBlockId();
    InetSocketAddress workerAddress = getQuery().getStage(ebId).getWorkerMap().get(workerId);

    try {
      tajoWorkerRpc = RpcClientManager.getInstance().getClient(workerAddress, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();
      tajoWorkerRpcClient.killTaskAttempt(null, taskAttemptId.getProto(), NullCallback.get());
    } catch (Exception e) {
      /* Worker RPC failure */
      LOG.error(e.getMessage(), e);
      queryMasterContext.getEventHandler().handle(new TaskFatalErrorEvent(taskAttemptId, e.getMessage()));
    }
  }

  private class LocalTaskEventHandler implements EventHandler<LocalTaskEvent> {
    @Override
    public void handle(final LocalTaskEvent event) {
      queryMasterContext.getEventExecutor().submit(new Runnable() {
        @Override
        public void run() {
          killTaskAttempt(event.getWorkerId(), event.getTaskAttemptId());
        }
      });
    }
  }

  private class QueryFinishEventHandler implements EventHandler<QueryMasterQueryCompletedEvent> {

    @Override
    public void handle(QueryMasterQueryCompletedEvent event) {

      QueryId queryId = event.getQueryId();
      LOG.info("Query completion notified from " + queryId + " final state: " + query.getSynchronizedState());
      queryMasterContext.getEventHandler().handle(new QueryStopEvent(queryId));
    }
  }

  private static boolean isTerminatedState(QueryState state) {
    return
        state == QueryState.QUERY_SUCCEEDED ||
        state == QueryState.QUERY_FAILED ||
        state == QueryState.QUERY_KILLED ||
        state == QueryState.QUERY_ERROR;
  }

  public synchronized void startQuery() {
    Tablespace sm = null;
    LogicalPlan plan = null;
    try {
      if (query != null) {
        LOG.warn("Query already started");
        return;
      }
      CatalogService catalog = getQueryTaskContext().getQueryMasterContext().getWorkerContext().getCatalog();
      LogicalPlanner planner = new LogicalPlanner(catalog);
      LogicalOptimizer optimizer = new LogicalOptimizer(systemConf);
      Expr expr = JsonHelper.fromJson(jsonExpr, Expr.class);
      jsonExpr = null; // remove the possible OOM
      plan = planner.createPlan(queryContext, expr);

      String storeType = PlannerUtil.getStoreType(plan);
      if (storeType != null) {
        sm = TableSpaceManager.getStorageManager(systemConf, storeType);
        StorageProperty storageProperty = sm.getStorageProperty();
        if (storageProperty.isSortedInsert()) {
          String tableName = PlannerUtil.getStoreTableName(plan);
          LogicalRootNode rootNode = plan.getRootBlock().getRoot();
          TableDesc tableDesc =  PlannerUtil.getTableDesc(catalog, rootNode.getChild());
          if (tableDesc == null) {
            throw new VerifyException("Can't get table meta data from catalog: " + tableName);
          }
          List<LogicalPlanRewriteRule> storageSpecifiedRewriteRules = sm.getRewriteRules(
              getQueryTaskContext().getQueryContext(), tableDesc);
          if (storageSpecifiedRewriteRules != null) {
            for (LogicalPlanRewriteRule eachRule: storageSpecifiedRewriteRules) {
              optimizer.addRuleAfterToJoinOpt(eachRule);
            }
          }
        }
      }

      optimizer.optimize(queryContext, plan);

      for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
        LogicalNode[] scanNodes = PlannerUtil.findAllNodes(block.getRoot(), NodeType.SCAN);
        if (scanNodes != null) {
          for (LogicalNode eachScanNode : scanNodes) {
            ScanNode scanNode = (ScanNode) eachScanNode;
            tableDescMap.put(scanNode.getCanonicalName(), scanNode.getTableDesc());
          }
        }

        scanNodes = PlannerUtil.findAllNodes(block.getRoot(), NodeType.PARTITIONS_SCAN);
        if (scanNodes != null) {
          for (LogicalNode eachScanNode : scanNodes) {
            ScanNode scanNode = (ScanNode) eachScanNode;
            tableDescMap.put(scanNode.getCanonicalName(), scanNode.getTableDesc());
          }
        }
      }
      MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
      queryMasterContext.getGlobalPlanner().build(queryContext, masterPlan);

      query = new Query(queryTaskContext, queryId, querySubmitTime,
          "", queryTaskContext.getEventHandler(), masterPlan);

      dispatcher.register(QueryEventType.class, query);
      queryTaskContext.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.START));
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      initError = t;

      if (plan != null && sm != null) {
        LogicalRootNode rootNode = plan.getRootBlock().getRoot();
        try {
          sm.rollbackOutputCommit(rootNode.getChild());
        } catch (IOException e) {
          LOG.warn(query.getId() + ", failed processing cleanup storage when query failed:" + e.getMessage(), e);
        }
      }
    }
  }

  private void initStagingDir() throws IOException {
    Path stagingDir;

    try {

      stagingDir = initStagingDir(systemConf, queryId.toString(), queryContext);

      // Create a subdirectories
      LOG.info("The staging dir '" + stagingDir + "' is created.");
      queryContext.setStagingDir(stagingDir);
    } catch (IOException ioe) {
      LOG.warn("Creating staging dir has been failed.", ioe);

      throw ioe;
    }
  }

  /**
   * It initializes the final output and staging directory and sets
   * them to variables.
   */
  public static Path initStagingDir(TajoConf conf, String queryId, QueryContext context) throws IOException {

    String realUser;
    String currentUser;
    UserGroupInformation ugi;
    ugi = UserGroupInformation.getLoginUser();
    realUser = ugi.getShortUserName();
    currentUser = UserGroupInformation.getCurrentUser().getShortUserName();

    FileSystem fs;
    Path stagingDir;

    ////////////////////////////////////////////
    // Create Output Directory
    ////////////////////////////////////////////

    String outputPath = context.get(QueryVars.OUTPUT_TABLE_PATH, "");
    if (context.isCreateTable() || context.isInsert()) {
      if (outputPath == null || outputPath.isEmpty()) {
        // hbase
        stagingDir = new Path(TajoConf.getDefaultRootStagingDir(conf), queryId);
      } else {
        stagingDir = StorageUtil.concatPath(context.getOutputPath(), TMP_STAGING_DIR_PREFIX, queryId);
      }
    } else {
      stagingDir = new Path(TajoConf.getDefaultRootStagingDir(conf), queryId);
    }

    // initializ
    fs = stagingDir.getFileSystem(conf);

    if (fs.exists(stagingDir)) {
      throw new IOException("The staging directory '" + stagingDir + "' already exists");
    }
    fs.mkdirs(stagingDir, new FsPermission(STAGING_DIR_PERMISSION));
    FileStatus fsStatus = fs.getFileStatus(stagingDir);
    String owner = fsStatus.getOwner();

    if (!owner.isEmpty() && !(owner.equals(currentUser) || owner.equals(realUser))) {
      throw new IOException("The ownership on the user's query " +
          "directory " + stagingDir + " is not as expected. " +
          "It is owned by " + owner + ". The directory must " +
          "be owned by the submitter " + currentUser + " or " +
          "by " + realUser);
    }

    if (!fsStatus.getPermission().equals(STAGING_DIR_PERMISSION)) {
      LOG.info("Permissions on staging directory " + stagingDir + " are " +
          "incorrect: " + fsStatus.getPermission() + ". Fixing permissions " +
          "to correct value " + STAGING_DIR_PERMISSION);
      fs.setPermission(stagingDir, new FsPermission(STAGING_DIR_PERMISSION));
    }

    Path stagingResultDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);
    fs.mkdirs(stagingResultDir);

    return stagingDir;
  }

  public Query getQuery() {
    return query;
  }

  protected void expireQuerySession() {
    if(!isTerminatedState(query.getState()) && !(query.getState() == QueryState.QUERY_KILL_WAIT)){
      query.handle(new QueryEvent(queryId, QueryEventType.KILL));
    }
  }

  public QueryMasterTaskContext getQueryTaskContext() {
    return queryTaskContext;
  }

  public EventHandler getEventHandler() {
    return queryTaskContext.getEventHandler();
  }

  public void touchSessionTime() {
    this.lastClientHeartbeat.set(System.currentTimeMillis());
  }

  public long getLastClientHeartbeat() {
    return this.lastClientHeartbeat.get();
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public boolean isInitError() {
    return initError != null;
  }

  public QueryState getState() {
    if(query == null) {
      if (isInitError()) {
        return QueryState.QUERY_ERROR;
      } else {
        return QueryState.QUERY_NOT_ASSIGNED;
      }
    } else {
      return query.getState();
    }
  }

  public Throwable getInitError() {
    return initError;
  }

  public String getErrorMessage() {
    if (isInitError()) {
      return StringUtils.stringifyException(initError);
    } else {
      return null;
    }
  }

  public long getQuerySubmitTime() {
    return this.querySubmitTime;
  }

  private void cleanupQuery(final QueryId queryId) {
    Set<InetSocketAddress> workers = Sets.newHashSet();
    for (Stage stage : getQuery().getStages()) {
      workers.addAll(stage.getWorkerMap().values());
    }

    LOG.info("Cleanup resources of all workers. Query: " + queryId + ", workers: " + workers.size());
    for (final InetSocketAddress worker : workers) {
      queryMasterContext.getEventExecutor().submit(new Runnable() {
        @Override
        public void run() {
          try {
            AsyncRpcClient rpc = RpcClientManager.getInstance().getClient(worker, TajoWorkerProtocol.class, true);
            TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerProtocolService = rpc.getStub();
            tajoWorkerProtocolService.cleanup(null, queryId.getProto(), NullCallback.get());
          } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
          }
        }
      });
    }
  }

  public class QueryMasterTaskContext {
    EventHandler eventHandler;
    public QueryMaster.QueryMasterContext getQueryMasterContext() {
      return queryMasterContext;
    }

    public Session getSession() {
      return session;
    }

    public QueryContext getQueryContext() {
      return queryContext;
    }

    public TajoConf getConf() {
      return systemConf;
    }

    public Clock getClock() {
      return queryMasterContext.getClock();
    }

    public Query getQuery() {
      return query;
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public Path getStagingDir() {
      return queryContext.getStagingDir();
    }

    public synchronized EventHandler getEventHandler() {
      if(eventHandler == null) {
        eventHandler = dispatcher.getEventHandler();
      }
      return eventHandler;
    }

    public AsyncDispatcher getDispatcher() {
      return dispatcher;
    }

    public Stage getStage(ExecutionBlockId id) {
      return query.getStage(id);
    }

    public Map<String, TableDesc> getTableDescMap() {
      return tableDescMap;
    }

    public float getProgress() {
      if(query == null) {
        return 0.0f;
      }
      return query.getProgress();
    }

    public TajoMetrics getQueryMetrics() {
      return queryMetrics;
    }

    /**
     * A key is worker id, and a value is a worker connection information.
     */
    public ConcurrentMap<Integer, WorkerConnectionInfo> getWorkerMap() {
      return workerMap;
    }
  }
}