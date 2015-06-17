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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.QueryCoordinatorProtocolService;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.TajoHeartbeat;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.TajoHeartbeatResponse;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.WorkerConnectionsProto;
import org.apache.tajo.master.event.QueryStartEvent;
import org.apache.tajo.master.event.QueryStopEvent;
import org.apache.tajo.rpc.*;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.history.QueryHistory;
import org.apache.tajo.worker.TajoWorker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QueryMaster extends CompositeService implements EventHandler {
  private static final Log LOG = LogFactory.getLog(QueryMaster.class.getName());

  private int querySessionTimeout;

  private Clock clock;

  private AsyncDispatcher dispatcher;

  private GlobalPlanner globalPlanner;

  private TajoConf systemConf;

  private Map<QueryId, QueryMasterTask> queryMasterTasks = Maps.newConcurrentMap();

  private Map<QueryId, QueryMasterTask> finishedQueryMasterTasks = Maps.newConcurrentMap();

  private ClientSessionTimeoutCheckThread clientSessionTimeoutCheckThread;

  private volatile boolean isStopped;

  private QueryMasterContext queryMasterContext;

  private QueryContext queryContext;

  private QueryHeartbeatThread queryHeartbeatThread;

  private FinishedQueryMasterTaskCleanThread finishedQueryMasterTaskCleanThread;

  private TajoWorker.WorkerContext workerContext;

  private RpcClientManager manager;

  private ExecutorService eventExecutor;

  public QueryMaster(TajoWorker.WorkerContext workerContext) {
    super(QueryMaster.class.getName());
    this.workerContext = workerContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    this.systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    this.manager = RpcClientManager.getInstance();

    querySessionTimeout = systemConf.getIntVar(TajoConf.ConfVars.QUERY_SESSION_TIMEOUT);
    queryMasterContext = new QueryMasterContext(systemConf);

    clock = new SystemClock();

    this.dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    globalPlanner = new GlobalPlanner(systemConf, workerContext);

    dispatcher.register(QueryStartEvent.EventType.class, new QueryStartEventHandler());
    dispatcher.register(QueryStopEvent.EventType.class, new QueryStopEventHandler());
    super.serviceInit(conf);
    LOG.info("QueryMaster inited");
  }

  @Override
  public void serviceStart() throws Exception {
    queryHeartbeatThread = new QueryHeartbeatThread();
    queryHeartbeatThread.start();

    clientSessionTimeoutCheckThread = new ClientSessionTimeoutCheckThread();
    clientSessionTimeoutCheckThread.start();

    finishedQueryMasterTaskCleanThread = new FinishedQueryMasterTaskCleanThread();
    finishedQueryMasterTaskCleanThread.start();

    eventExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    super.serviceStart();
    LOG.info("QueryMaster started");
  }

  @Override
  public void serviceStop() throws Exception {
    isStopped = true;

    if(queryHeartbeatThread != null) {
      queryHeartbeatThread.interrupt();
    }

    if(clientSessionTimeoutCheckThread != null) {
      clientSessionTimeoutCheckThread.interrupt();
    }

    if(finishedQueryMasterTaskCleanThread != null) {
      finishedQueryMasterTaskCleanThread.interrupt();
    }

    if(eventExecutor != null){
      eventExecutor.shutdown();
    }

    super.serviceStop();
    LOG.info("QueryMaster stopped");
  }

  public List<TajoProtos.WorkerConnectionInfoProto> getAllWorker() {

    NettyClientBase rpc = null;
    try {
      // In TajoMaster HA mode, if backup master be active status,
      // worker may fail to connect existing active master. Thus,
      // if worker can't connect the master, worker should try to connect another master and
      // update master address in worker context.

      ServiceTracker serviceTracker = workerContext.getServiceTracker();
      rpc = manager.getClient(serviceTracker.getUmbilicalAddress(), QueryCoordinatorProtocol.class, true);
      QueryCoordinatorProtocolService masterService = rpc.getStub();

      CallFuture<WorkerConnectionsProto> callBack = new CallFuture<WorkerConnectionsProto>();
      masterService.getAllWorkers(callBack.getController(),
          PrimitiveProtos.NullProto.getDefaultInstance(), callBack);

      WorkerConnectionsProto connectionsProto =
          callBack.get(RpcConstants.DEFAULT_FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      return connectionsProto.getWorkerList();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    return new ArrayList<TajoProtos.WorkerConnectionInfoProto>();
  }

  @Override
  public void handle(Event event) {
    dispatcher.getEventHandler().handle(event);
  }

  public Query getQuery(QueryId queryId) {
    return queryMasterTasks.get(queryId).getQuery();
  }

  public QueryMasterTask getQueryMasterTask(QueryId queryId) {
    return queryMasterTasks.get(queryId);
  }

  public QueryMasterTask getQueryMasterTask(QueryId queryId, boolean includeFinished) {
    QueryMasterTask queryMasterTask =  queryMasterTasks.get(queryId);
    if(queryMasterTask != null) {
      return queryMasterTask;
    } else {
      if(includeFinished) {
        return finishedQueryMasterTasks.get(queryId);
      } else {
        return null;
      }
    }
  }

  public QueryMasterContext getContext() {
    return this.queryMasterContext;
  }

  public Collection<QueryMasterTask> getQueryMasterTasks() {
    return queryMasterTasks.values();
  }

  public Collection<QueryMasterTask> getFinishedQueryMasterTasks() {
    return finishedQueryMasterTasks.values();
  }

  public class QueryMasterContext {
    private TajoConf conf;

    public QueryMasterContext(TajoConf conf) {
      this.conf = conf;
    }

    public TajoConf getConf() {
      return conf;
    }

    public ExecutorService getEventExecutor(){
      return eventExecutor;
    }

    public AsyncDispatcher getDispatcher() {
      return dispatcher;
    }

    public Clock getClock() {
      return clock;
    }

    public QueryMaster getQueryMaster() {
      return QueryMaster.this;
    }

    public GlobalPlanner getGlobalPlanner() {
      return globalPlanner;
    }

    public TajoWorker.WorkerContext getWorkerContext() {
      return workerContext;
    }

    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    public void stopQuery(QueryId queryId) {
      QueryMasterTask queryMasterTask = queryMasterTasks.remove(queryId);
      if(queryMasterTask == null) {
        LOG.warn("No query info:" + queryId);
        return;
      }

      finishedQueryMasterTasks.put(queryId, queryMasterTask);

      TajoHeartbeat queryHeartbeat = buildTajoHeartBeat(queryMasterTask);
      CallFuture<TajoHeartbeatResponse> future = new CallFuture<TajoHeartbeatResponse>();

      NettyClientBase tmClient;
      try {
        tmClient = manager.getClient(workerContext.getServiceTracker().getUmbilicalAddress(),
            QueryCoordinatorProtocol.class, true);

        QueryCoordinatorProtocolService masterClientService = tmClient.getStub();
        masterClientService.heartbeat(future.getController(), queryHeartbeat, future);
        future.get(RpcConstants.DEFAULT_FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }  catch (Exception e) {
        //this function will be closed in new thread.
        //When tajo do stop cluster, tajo master maybe throw closed connection exception

        LOG.error(e.getMessage(), e);
      }

      try {
        queryMasterTask.stop();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
      Query query = queryMasterTask.getQuery();
      if (query != null) {
        QueryHistory queryHisory = query.getQueryHistory();
        if (queryHisory != null) {
          try {
            long startTime = System.currentTimeMillis();
            query.context.getQueryMasterContext().getWorkerContext().
                getTaskHistoryWriter().appendAndFlush(queryHisory);
            LOG.info("QueryHistory write delay:" + (System.currentTimeMillis() - startTime));
          } catch (Throwable e) {
            LOG.warn(e, e);
          }
        }
      }
    }
  }

  private TajoHeartbeat buildTajoHeartBeat(QueryMasterTask queryMasterTask) {
    TajoHeartbeat.Builder builder = TajoHeartbeat.newBuilder();

    builder.setConnectionInfo(workerContext.getConnectionInfo().getProto());
    builder.setQueryId(queryMasterTask.getQueryId().getProto());
    builder.setState(queryMasterTask.getState());
    if (queryMasterTask.getQuery() != null) {
      if (queryMasterTask.getQuery().getResultDesc() != null) {
        builder.setResultDesc(queryMasterTask.getQuery().getResultDesc().getProto());
      }
      builder.setQueryProgress(queryMasterTask.getQuery().getProgress());
    }
    return builder.build();
  }

  private class QueryStartEventHandler implements EventHandler<QueryStartEvent> {
    @Override
    public void handle(QueryStartEvent event) {
      LOG.info("Start QueryStartEventHandler:" + event.getQueryId());
      QueryMasterTask queryMasterTask = new QueryMasterTask(queryMasterContext,
          event.getQueryId(), event.getSession(), event.getQueryContext(), event.getJsonExpr(), event.getAllocation());

      synchronized(queryMasterTasks) {
        queryMasterTasks.put(event.getQueryId(), queryMasterTask);
      }

      queryMasterTask.init(systemConf);
      if (!queryMasterTask.isInitError()) {
        queryMasterTask.start();
      }

      queryContext = event.getQueryContext();

      if (queryMasterTask.isInitError()) {
        queryMasterContext.stopQuery(queryMasterTask.getQueryId());
      }
    }
  }

  private class QueryStopEventHandler implements EventHandler<QueryStopEvent> {
    @Override
    public void handle(QueryStopEvent event) {
      queryMasterContext.stopQuery(event.getQueryId());
    }
  }

  class QueryHeartbeatThread extends Thread {
    public QueryHeartbeatThread() {
      super("QueryHeartbeatThread");
    }

    @Override
    public void run() {
      LOG.info("Start QueryMaster heartbeat thread");
      while(!isStopped) {
        List<QueryMasterTask> tempTasks = new ArrayList<QueryMasterTask>();
        tempTasks.addAll(queryMasterTasks.values());

        for(QueryMasterTask eachTask: tempTasks) {
          NettyClientBase tmClient;
          try {

            ServiceTracker serviceTracker = queryMasterContext.getWorkerContext().getServiceTracker();
            tmClient = manager.getClient(serviceTracker.getUmbilicalAddress(),
                QueryCoordinatorProtocol.class, true);
            QueryCoordinatorProtocolService masterClientService = tmClient.getStub();

            TajoHeartbeat queryHeartbeat = buildTajoHeartBeat(eachTask);
            masterClientService.heartbeat(null, queryHeartbeat, NullCallback.get());
          } catch (Throwable t) {
            t.printStackTrace();
          }
        }

        synchronized(this) {
          try {
            this.wait(2000);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
      LOG.info("QueryMaster heartbeat thread stopped");
    }
  }

  class ClientSessionTimeoutCheckThread extends Thread {
    public void run() {
      LOG.info("ClientSessionTimeoutCheckThread started");
      while(!isStopped) {
        try {
          synchronized (this) {
            this.wait(1000);
          }
        } catch (InterruptedException e) {
          break;
        }
        List<QueryMasterTask> tempTasks = new ArrayList<QueryMasterTask>();
        tempTasks.addAll(queryMasterTasks.values());

        for(QueryMasterTask eachTask: tempTasks) {
          if(!eachTask.isStopped()) {
            try {
              long lastHeartbeat = eachTask.getLastClientHeartbeat();
              long time = System.currentTimeMillis() - lastHeartbeat;
              if(lastHeartbeat > 0 && time > querySessionTimeout * 1000) {
                LOG.warn("Query " + eachTask.getQueryId() + " stopped cause query session timeout: " + time + " ms");
                eachTask.expireQuerySession();
              }
            } catch (Exception e) {
              LOG.error(eachTask.getQueryId() + ":" + e.getMessage(), e);
            }
          }
        }
      }
    }
  }

  class FinishedQueryMasterTaskCleanThread extends Thread {
    public void run() {
      int expireIntervalTime = systemConf.getIntVar(TajoConf.ConfVars.QUERYMASTER_HISTORY_EXPIRE_PERIOD);
      LOG.info("FinishedQueryMasterTaskCleanThread started: expire interval minutes = " + expireIntervalTime);
      while(!isStopped) {
        try {
          synchronized (this) {
            this.wait(60 * 1000);  // minimum interval minutes
          }
        } catch (InterruptedException e) {
          break;
        }
        try {
          long expireTime = System.currentTimeMillis() - expireIntervalTime * 60 * 1000l;
          cleanExpiredFinishedQueryMasterTask(expireTime);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }

    private void cleanExpiredFinishedQueryMasterTask(long expireTime) {
      synchronized(finishedQueryMasterTasks) {
        List<QueryId> expiredQueryIds = new ArrayList<QueryId>();
        for(Map.Entry<QueryId, QueryMasterTask> entry: finishedQueryMasterTasks.entrySet()) {

          /* If a query are abnormal termination, the finished time will be zero. */
          long finishedTime = entry.getValue().getStartTime();
          Query query = entry.getValue().getQuery();
          if (query != null && query.getFinishTime() > 0) {
            finishedTime = query.getFinishTime();
          }

          if(finishedTime < expireTime) {
            expiredQueryIds.add(entry.getKey());
          }
        }

        for(QueryId eachId: expiredQueryIds) {
          finishedQueryMasterTasks.remove(eachId);
        }
      }
    }
  }
}
