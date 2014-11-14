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
import com.google.common.collect.Queues;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.TaskRunnerGroupEvent;
import org.apache.tajo.master.TaskRunnerLauncher;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.event.ContainerAllocationEvent;
import org.apache.tajo.master.event.ContainerAllocatorEventType;
import org.apache.tajo.master.event.SubQueryContainerAllocationEvent;
import org.apache.tajo.master.event.WorkerResourceRequestEvent;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.master.querymaster.SubQueryState;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.scheduler.AbstractScheduler;
import org.apache.tajo.scheduler.FairScheduler;
import org.apache.tajo.scheduler.Scheduler;
import org.apache.tajo.scheduler.SchedulingAlgorithms;
import org.apache.tajo.util.HAServiceUtil;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ipc.TajoMasterProtocol.WorkerResourceAllocationResponse;

public class TajoResourceAllocator extends AbstractResourceAllocator {
  private static final Log LOG = LogFactory.getLog(TajoResourceAllocator.class);

  private TajoConf tajoConf;
  private QueryMasterTask.QueryMasterTaskContext queryTaskContext;
  private final ExecutorService executorService;

  /**
   * A key is a worker unique id, and a value is allocated worker resources.
   */
  private ConcurrentMap<Integer, ConcurrentLinkedQueue<TajoMasterProtocol.AllocatedWorkerResourceProto>> allocatedResourceMap =
      Maps.newConcurrentMap();
  /** allocated resources and not released  */
  private AtomicInteger allocatedSize = new AtomicInteger(0); //TODO handle from scheduler


  private WorkerResourceAllocator allocatorThread;
  private ResourceDeAllocator deAllocator;

  private static ConcurrentMap<String, AbstractScheduler.QueueProperty> queuePropertyMap = Maps.newConcurrentMap();

  public TajoResourceAllocator(QueryMasterTask.QueryMasterTaskContext queryTaskContext) {
    this.queryTaskContext = queryTaskContext;
    this.executorService = Executors.newFixedThreadPool(
        queryTaskContext.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM));
  }

  static {
    loadSchedulerProperties(new TajoConf());
  }

  private static void loadSchedulerProperties(TajoConf conf) {
    Scheduler scheduler = null;
    try {
      scheduler = SchedulingAlgorithms.getScheduler(conf);
      LOG.info("Load scheduler : " + scheduler.getMode());
      if(scheduler instanceof FairScheduler){
        FairScheduler fairScheduler = (FairScheduler)scheduler;
        List<AbstractScheduler.QueueProperty> queueProperties = fairScheduler.loadQueueProperty(conf);
        for (AbstractScheduler.QueueProperty queueProperty : queueProperties) {
          queuePropertyMap.put(queueProperty.getQueueName(), queueProperty);
        }
      }
    } catch (Exception e) {
      LOG.error(e);
      throw new RuntimeException(e);
    } finally {
      if(scheduler != null){
        scheduler.stop();
      }
    }
  }

  @Override
  public void init(Configuration conf) {
    tajoConf = (TajoConf) conf;

    queryTaskContext.getDispatcher().register(TaskRunnerGroupEvent.EventType.class, new TajoTaskRunnerLauncher());
    queryTaskContext.getDispatcher().register(ContainerAllocatorEventType.class, new TajoWorkerAllocationHandler());
    queryTaskContext.getDispatcher().register(WorkerResourceRequestEvent.EventType.class, new WorkerResourceHandler());

    loadSchedulerProperties(tajoConf);
    allocatorThread = new WorkerResourceAllocator(this);
    deAllocator = new ResourceDeAllocator();
    super.init(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    allocatorThread.shutdown();

    for (int workerId : allocatedResourceMap.keySet()){
      try{
        releaseWorkerResource(queryTaskContext.getQueryId(), workerId);
      } catch (Throwable t){
        LOG.fatal(t.getMessage(), t);
      }
    }
    allocatedResourceMap.clear();
    allocatedSize.set(0);
    executorService.shutdownNow();
    deAllocator.shutdown();
    super.serviceStop();
    LOG.info("Tajo Resource Allocator stopped");
  }

  @Override
  public void start() {
    allocatorThread.start();
    deAllocator.start();
    super.start();
  }

  @Override
  public int calculateNumRequestContainers(TajoWorker.WorkerContext workerContext,
                                           int numTasks,
                                           int memoryMBPerTask,
                                           boolean isLeaf) {
    TajoMasterProtocol.ClusterResourceSummary clusterResource = workerContext.getClusterResource();
    int clusterSlots;
    if(isLeaf){
      int diskSlots = clusterResource == null ? 0 : (int)(clusterResource.getTotalDiskSlots() / 0.5f);
      int memSlots = clusterResource == null ? 0 : clusterResource.getTotalMemoryMB() / memoryMBPerTask;
      clusterSlots = Math.min(diskSlots, memSlots);
    } else {
      clusterSlots = clusterResource == null ? 0 : clusterResource.getTotalMemoryMB() / memoryMBPerTask;
    }
    clusterSlots = Math.max(1, clusterSlots - 1); // reserve query master slot
    LOG.info("CalculateNumberRequestContainer - Number of Tasks=" + numTasks +
        ", Number of Cluster Slots=" + clusterSlots);
    return Math.min(numTasks, clusterSlots);
  }

  class TajoTaskRunnerLauncher implements TaskRunnerLauncher {
    @Override
    public void handle(TaskRunnerGroupEvent event) {
      if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_LAUNCH) {
        //LaunchTaskRunnersEvent launchEvent = (LaunchTaskRunnersEvent) event;
        //launchTaskRunners(launchEvent);
      } else if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_CLEANUP) {
        allocatorThread.stopWorkerResourceAllocator(event.getExecutionBlockId());
        stopExecutionBlock(event.getExecutionBlockId());
      }
    }
  }

  private void launchTaskRunners(final ContainerAllocationEvent event, final Map<Integer, Integer> allocatedResources) {
    // Query in standby mode doesn't need launch Worker.
    // But, Assign ExecutionBlock to assigned tajo worker
    List<Integer> workerIds = Lists.newArrayList(allocatedResources.keySet());
    Collections.shuffle(workerIds);
    for (final int workerId : workerIds) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
         executeExecutionBlock(event, workerId, allocatedResources.get(workerId));
        }
      });
    }
  }

  public void stopExecutionBlock(final ExecutionBlockId executionBlockId) {
    for (final int workerId : workerInfoMap.keySet()) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          releaseWorkerResource(queryTaskContext.getQueryId(), workerId);
          stopExecutionBlock(executionBlockId, workerId);
        }
      });
    }
  }



  /**
   * It sends a release rpc request to the resource manager.
   *
   * @param workerId a worker id.
   * @param executionBlockId
   * @param resources resource size
   */
  @Override
  public void releaseWorkerResource(final ExecutionBlockId executionBlockId, final int workerId, final int resources) {
    ConcurrentLinkedQueue<TajoMasterProtocol.AllocatedWorkerResourceProto>
        allocatedWorkerResources = allocatedResourceMap.get(workerId);
    if (allocatedWorkerResources == null) return;

    synchronized (allocatedWorkerResources) {
      if (allocatedWorkerResources.size() == resources) {
        allocatedResourceMap.remove(workerId);
        deAllocator.releseResources(allocatedWorkerResources);
        allocatedWorkerResources.clear();
      } else {
        for (int i = 0; i < resources; i++) {
          TajoMasterProtocol.AllocatedWorkerResourceProto allocatedWorkerResourceProto =
              allocatedWorkerResources.poll();
          if (allocatedWorkerResourceProto == null) {
            break;
          }
          deAllocator.releseResource(allocatedWorkerResourceProto);
        }
      }
    }
  }

  private void releaseWorkerResource(final QueryId queryId, final int workerId) {
    final ConcurrentLinkedQueue<TajoMasterProtocol.AllocatedWorkerResourceProto> resources = allocatedResourceMap.remove(workerId);
    if (resources != null && resources.size() > 0) {
      final List<TajoMasterProtocol.AllocatedWorkerResourceProto> requestList = Lists.newArrayList(resources);
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Release Worker: " + workerId + ", QueryId : " + queryId + ", Resources: " + requestList);
          }
          releaseWorkerResourceRequest(queryId, requestList);
        }
      });
    }
  }

  /**
   * It sends a kill RPC request to a corresponding worker.
   *
   * @param workerId a worker id.
   * @param taskAttemptId The TaskAttemptId to be killed.
   */
  @Override
  public void killTaskAttempt(int workerId, QueryUnitAttemptId taskAttemptId) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      WorkerConnectionInfo connectionInfo = getWorkerConnectionInfo(workerId);
      InetSocketAddress addr = new InetSocketAddress(connectionInfo.getHost(), connectionInfo.getPeerRpcPort());
      tajoWorkerRpc = RpcConnectionPool.getPool(tajoConf).getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();
      tajoWorkerRpcClient.killTaskAttempt(null, taskAttemptId.getProto(), NullCallback.get());

      releaseWorkerResource(taskAttemptId.getQueryUnitId().getExecutionBlockId(), workerId, 1);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool(tajoConf).releaseConnection(tajoWorkerRpc);
    }
  }

  private void executeExecutionBlock(ContainerAllocationEvent event, int workerId, int launchTasks) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      WorkerConnectionInfo connectionInfo = getWorkerConnectionInfo(workerId);
      if(connectionInfo == null) return;

      InetSocketAddress addr = new InetSocketAddress(connectionInfo.getHost(), connectionInfo.getPeerRpcPort());
      tajoWorkerRpc = RpcConnectionPool.getPool(tajoConf).getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();

      WorkerConnectionInfo queryMaster = queryTaskContext.getQueryMasterContext().getWorkerContext().getConnectionInfo();
      TajoWorkerProtocol.RunExecutionBlockRequestProto request =
          TajoWorkerProtocol.RunExecutionBlockRequestProto.newBuilder()
              .setExecutionBlockId(event.getExecutionBlockId().getProto())
              .setQueryMaster(queryMaster.getProto())
              .setTasks(launchTasks)
              .setQueryOutputPath(queryTaskContext.getStagingDir().toString())
              .setQueryContext(event.getQueryContext().getProto())
              .setPlanJson(event.getPlanJson())
              .build();

      tajoWorkerRpcClient.startExecutionBlock(null, request, NullCallback.get());
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool(tajoConf).releaseConnection(tajoWorkerRpc);
    }
  }

  private void stopExecutionBlock(ExecutionBlockId executionBlockId, int workerId) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      WorkerConnectionInfo connectionInfo = getWorkerConnectionInfo(workerId);

      InetSocketAddress addr = new InetSocketAddress(connectionInfo.getHost(), connectionInfo.getPeerRpcPort());
      tajoWorkerRpc = RpcConnectionPool.getPool(tajoConf).getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();

      tajoWorkerRpcClient.stopExecutionBlock(null, executionBlockId.getProto(), NullCallback.get());
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool(tajoConf).releaseConnection(tajoWorkerRpc);
    }
  }

  private void releaseWorkerResourceRequest(QueryId queryId,
                                      List<TajoMasterProtocol.AllocatedWorkerResourceProto> resources) {
    if (resources == null || resources.size() == 0) return;

    allocatedSize.getAndAdd(-resources.size());
    try {
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = getMasterStub();
      masterClientService.releaseWorkerResource(null,
          TajoMasterProtocol.WorkerResourceReleaseProto.newBuilder()
              .setQueryId(queryId.getProto())
              .addAllResources(resources)
              .build(),
          NullCallback.get()
      );
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private TajoMasterProtocol.TajoMasterProtocolService getMasterStub() {
    RpcConnectionPool connPool = RpcConnectionPool.getPool(queryTaskContext.getConf());
    NettyClientBase tmClient = null;
    TajoMasterProtocol.TajoMasterProtocolService masterClientService = null;
    try {
      // In TajoMaster HA mode, if backup master be active status,
      // worker may fail to connect existing active master. Thus,
      // if worker can't connect the master, worker should try to connect another master and
      // update master address in worker context.
      TajoConf conf = queryTaskContext.getConf();
      if (conf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
        try {
          tmClient = connPool.getConnection(queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
              TajoMasterProtocol.class, true);
        } catch (Exception e) {
          queryTaskContext.getQueryMasterContext().getWorkerContext().setWorkerResourceTrackerAddr(
              HAServiceUtil.getResourceTrackerAddress(conf));
          queryTaskContext.getQueryMasterContext().getWorkerContext().setTajoMasterAddress(
              HAServiceUtil.getMasterUmbilicalAddress(conf));
          tmClient = connPool.getConnection(queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
              TajoMasterProtocol.class, true);
        }
      } else {
        tmClient = connPool.getConnection(queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
            TajoMasterProtocol.class, true);
      }

      masterClientService = tmClient.getStub();
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    } finally {
      connPool.releaseConnection(tmClient);
    }
    return masterClientService;
  }

  class WorkerResourceHandler implements EventHandler<WorkerResourceRequestEvent> {
    @Override
    public void handle(WorkerResourceRequestEvent event) {
      if (event.getType() == WorkerResourceRequestEvent.EventType.CONTAINER_RELEASE) {
        releaseWorkerResource(event.getExecutionBlockId(), event.getWorkerId(), 1);
      }
    }
  }

  class TajoWorkerAllocationHandler implements EventHandler<ContainerAllocationEvent> {

    @Override
    public void handle(ContainerAllocationEvent event) {
      allocatorThread.startWorkerResourceAllocator(event);
    }
  }

  private TajoMasterProtocol.WorkerResourcesRequestProto createWorkerResourcesRequest(TajoIdProtos.QueryIdProto queryIdProto,
                                                                                      int wokerResource,
                                                                                      TajoMasterProtocol.ResourceRequestPriority requestPriority,
                                                                                      List<Integer> workerIds) {
    //TODO consider task's resource usage pattern
    int requiredMemoryMB = tajoConf.getIntVar(TajoConf.ConfVars.TASK_DEFAULT_MEMORY);
    float requiredDiskSlots = tajoConf.getFloatVar(TajoConf.ConfVars.TASK_DEFAULT_DISK);

    return TajoMasterProtocol.WorkerResourcesRequestProto.newBuilder()
        .setQueryId(queryIdProto)
        .setMinMemoryMBPerContainer(requiredMemoryMB)
        .setMaxMemoryMBPerContainer(requiredMemoryMB)
        .setNumContainers(wokerResource)
        .setResourceRequestPriority(requestPriority)
        .setMinDiskSlotPerContainer(requiredDiskSlots)
        .setMaxDiskSlotPerContainer(requiredDiskSlots)
        .addAllWorkerId(workerIds)
        .build();
  }

  private WorkerResourceAllocationResponse reserveWokerResources(ExecutionBlockId executionBlockId,
                                                                 int required,
                                                                 boolean isLeaf,
                                                                 List<Integer> workerIds) {
    TajoMasterProtocol.ResourceRequestPriority priority =
        isLeaf ? TajoMasterProtocol.ResourceRequestPriority.DISK : TajoMasterProtocol.ResourceRequestPriority.MEMORY;

    CallFuture<WorkerResourceAllocationResponse> callBack =
        new CallFuture<WorkerResourceAllocationResponse>();

    try {
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = getMasterStub();
      masterClientService.allocateWorkerResources(
          null,
          createWorkerResourcesRequest(executionBlockId.getQueryId().getProto(), required, priority, workerIds),
          callBack);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      return null;
    }

    WorkerResourceAllocationResponse response = null;
    while (!isInState(STATE.STOPPED)) {
      try {
        response = callBack.get(3, TimeUnit.SECONDS);
        break;
      } catch (InterruptedException e) {
        if (isInState(STATE.STOPPED)) {
          return null;
        }
      } catch (TimeoutException e) {
        LOG.info("No available worker resource for " + queryTaskContext.getQueryId() +
            ", allocated resources : " + allocatedSize.get());

        continue;
      }
    }
    return response;
  }

  private int getRunningQueries(String queueName) {

    int runningSize = -1;
    CallFuture<PrimitiveProtos.IntProto> callBack = new CallFuture<PrimitiveProtos.IntProto>();
    try {
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = getMasterStub();
      masterClientService.getRunningQueries(callBack.getController(),
          PrimitiveProtos.StringProto.newBuilder().setValue(queueName).build(),
          callBack);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return runningSize;
    }

    while (!isInState(STATE.STOPPED)) {
      try {
        PrimitiveProtos.IntProto response = callBack.get(3, TimeUnit.SECONDS);
        runningSize = response.getValue();
        return runningSize;
      } catch (InterruptedException e) {
        if (isInState(STATE.STOPPED)) {
          return runningSize;
        }
      } catch (TimeoutException e) {
        if(callBack.getController().failed()){
          return runningSize;
        }
        continue;
      }
    }
    return runningSize;
  }

  private List<Integer> getWorkerIds(Collection<String> hosts){
    List<Integer> workerIds = Lists.newArrayList();
    if(hosts.isEmpty()) return workerIds;

    List<TajoMasterProtocol.WorkerResourceProto> workers =
        queryTaskContext.getQueryMasterContext().getQueryMaster().getAllWorker();
    for (TajoMasterProtocol.WorkerResourceProto worker : workers) {
      if(hosts.contains(worker.getConnectionInfo().getHost())){
        workerIds.add(worker.getConnectionInfo().getId());
      }
    }
    return workerIds;
  }

  class WorkerResourceAllocator extends Thread {
    final int delay = 50;
    final int updateInterval = 1000;
    private AtomicBoolean stop = new AtomicBoolean(false);
    final TajoResourceAllocator allocator;
    final BlockingDeque<WorkerResourceRequest> queue =
        new LinkedBlockingDeque<WorkerResourceRequest>();

    class WorkerResourceRequest {
      AtomicBoolean isFirst = new AtomicBoolean(true);
      AtomicBoolean stop = new AtomicBoolean();
      ContainerAllocationEvent event;
      long updatedTime;
      AbstractScheduler.QueueProperty queueProperty;
      int runningInQueue = 0;
      List<Integer> workerIds;

      public WorkerResourceRequest(ContainerAllocationEvent event, List<Integer> workerIds,
                                   AbstractScheduler.QueueProperty queueProperty) {
        this.event = event;
        this.workerIds = workerIds;
        this.queueProperty = queueProperty;
        this.updatedTime = System.currentTimeMillis();
        if (queueProperty != null) {
          this.runningInQueue = allocator.getRunningQueries(queueProperty.getQueueName());
        } else {
          this.runningInQueue = 0;
        }
      }
    }

    public WorkerResourceAllocator(TajoResourceAllocator allocator) {
      this.allocator = allocator;
    }

    public void startWorkerResourceAllocator(ContainerAllocationEvent event) {
      try {
        LOG.info("Start allocation. required containers(" + event.getRequiredNum() + ") executionBlockId : " + event.getExecutionBlockId());

        for (int workerId : allocatedResourceMap.keySet()) {
          if (allocatedResourceMap.containsKey(workerId)) {
            try {
              releaseWorkerResource(queryTaskContext.getQueryId(), workerId);
            } catch (Throwable t) {
              LOG.fatal(t.getMessage(), t);
            }
          }
        }

        List<Integer> workerIds;
        if(event.isLeafQuery()){
          Set<String> hosts = allocator.queryTaskContext.getSubQuery(event.getExecutionBlockId()).getTaskScheduler().getLeafTaskHosts();
          workerIds = getWorkerIds(hosts);
        } else {
          workerIds = Lists.newArrayList();
        }

        AbstractScheduler.QueueProperty queueProperty =
            queuePropertyMap.get(queryTaskContext.getQueryContext().get(Scheduler.QUERY_QUEUE_KEY, Scheduler.DEFAULT_QUEUE_NAME));

        queue.put(new WorkerResourceRequest(event, workerIds, queueProperty));

        deAllocator.startDeAllocator(queryTaskContext.getQueryId());
      } catch (InterruptedException e) {
        if (!stop.get()) {
          LOG.warn("ContainerAllocator thread interrupted");
        }
      }
    }

    public void stopWorkerResourceAllocator(ExecutionBlockId executionBlockId) {
      if (queue.size() > 0) {
        Iterator<WorkerResourceRequest> iterator = queue.iterator();
        while (iterator.hasNext()){
          WorkerResourceRequest request = iterator.next();
          if(request.event.getExecutionBlockId().equals(executionBlockId)){
            request.stop.set(true);
            iterator.remove();
            LOG.warn("Container allocator force stopped. executionBlockId : " + request.event.getExecutionBlockId());
          }
        }
      }

      if (allocatorThread != null) {
        synchronized (allocatorThread) {
          allocatorThread.notifyAll();
        }
      }
    }

    public synchronized void shutdown() {
      if (stop.getAndSet(true)) {
        return;
      }

      if (queue.size() > 0) {
        Iterator<WorkerResourceRequest> iterator = queue.iterator();
        while (iterator.hasNext()){
          WorkerResourceRequest request = iterator.next();
          request.stop.set(true);
        }
      }

      if (allocatorThread != null) {
        allocatorThread.interrupt();
      }
    }

    @Override
    public void run() {
      while (!stop.get() && !Thread.currentThread().isInterrupted()) {
        WorkerResourceRequest request;
        try {
          request = queue.take();
        } catch (InterruptedException ie) {
          if (!stop.get()) {
            LOG.warn("ContainerAllocator thread interrupted");
          }
          break;
        }
        ExecutionBlockId executionBlockId = request.event.getExecutionBlockId();
        SubQueryState state = queryTaskContext.getSubQuery(executionBlockId).getSynchronizedState();
        int remainingTask = allocator.queryTaskContext.getSubQuery(executionBlockId).getTaskScheduler().remainingScheduledObjectNum();
        /* for scheduler */

        int resources = request.event.getRequiredNum();
        if (request.queueProperty != null && request.queueProperty.getMaxCapacity() > 0) {
          if(request.updatedTime + updateInterval < System.currentTimeMillis()){
            // update the master scheduler status
            int runningSize = allocator.getRunningQueries(request.queueProperty.getQueueName());
            request.runningInQueue = runningSize > 0 ? runningSize : 1;
            LOG.debug(String.format("runningInQueue: %d, MaxCapacity: %d", request.runningInQueue, request.queueProperty.getMaxCapacity()));
          }
          int demandSize = (int) Math.floor(request.queueProperty.getMaxCapacity() / request.runningInQueue);
          int share = request.queueProperty.getMinCapacity() * (request.runningInQueue - 1);
          int availableSize = request.queueProperty.getMaxCapacity() - share;
          demandSize = Math.max(availableSize, demandSize);

          resources = Math.min(resources, demandSize - allocatedSize.get());
          request.updatedTime = System.currentTimeMillis();
        } else {
          resources = resources - allocatedSize.get();
        }

        try {
          if (!request.stop.get() && SubQuery.isRunningState(state)) {
            queue.addFirst(request);
            if(LOG.isDebugEnabled()){
              LOG.debug("Retry to allocate containers executionBlockId : " + request.event.getExecutionBlockId());
            }

            if (remainingTask <= 0) {
              // in order to reallocate, if a QueryUnitAttempt was failure
              LOG.debug("All Allocated. executionBlockId : " + request.event.getExecutionBlockId());
            } else {
              int determinedResources = Math.min(remainingTask * 2, resources); // for tail tasks
              if(LOG.isDebugEnabled()){
                LOG.debug(String.format("requiredNum: %d, allocatedSize: %d, remainingTask: %d", resources, allocatedSize.get(), remainingTask));
              }
              allocateContainers(request, determinedResources);
            }

            if(!request.stop.get()){
              synchronized (allocatorThread) {
                allocatorThread.wait(delay);
              }
            }
          } else {
            LOG.warn("ExecutionBlock is not running state : " + state + ", " + executionBlockId);
          }
        } catch (InterruptedException e) {
          if (!stop.get()) {
            LOG.warn("ContainerAllocator thread interrupted");
          }
          break;
        }
      }
      LOG.info("ContainerAllocator Stopped");
    }

    private void allocateContainers(WorkerResourceRequest request, int resources) {
      ExecutionBlockId executionBlockId = request.event.getExecutionBlockId();

      if(LOG.isDebugEnabled()){
        LOG.debug("Try to allocate containers executionBlockId : " + executionBlockId + "," + resources);
      }
      if(resources <= 0) return;

      List<Integer> workerIds;
      if(request.event.isLeafQuery() && request.isFirst.get()){
        workerIds = request.workerIds;
      } else {
        workerIds = Lists.newArrayList();
      }
      WorkerResourceAllocationResponse response =
          reserveWokerResources(executionBlockId, resources, request.event.isLeafQuery(), workerIds);
      if (response != null) {
        List<TajoMasterProtocol.AllocatedWorkerResourceProto> allocatedResources = response.getAllocatedWorkerResourceList();
        allocatedSize.addAndGet(allocatedResources.size());

        Map<Integer, Integer> tasksLaunchMap = Maps.newHashMap();
        for (TajoMasterProtocol.AllocatedWorkerResourceProto eachAllocatedResource : allocatedResources) {
          WorkerConnectionInfo connectionInfo = new WorkerConnectionInfo(eachAllocatedResource.getWorker().getConnectionInfo());
          int workerId = connectionInfo.getId();
          addWorkerConnectionInfo(connectionInfo);

          synchronized (allocatedResourceMap){
            allocatedResourceMap.putIfAbsent(workerId, new ConcurrentLinkedQueue<TajoMasterProtocol.AllocatedWorkerResourceProto>());
            allocatedResourceMap.get(workerId).add(eachAllocatedResource);
          }

          if(!tasksLaunchMap.containsKey(workerId)){
            tasksLaunchMap.put(workerId, 0);
          }
          tasksLaunchMap.put(workerId, tasksLaunchMap.get(workerId) + 1);
        }

        if (!request.stop.get() && allocatedResources.size() > 0) {
          LOG.info("Reserved worker resources : " + allocatedResources.size()
              + ", EBId : " + executionBlockId);
          LOG.debug("SubQueryContainerAllocationEvent fire:" + executionBlockId);

          if (LOG.isDebugEnabled()) {
            LOG.debug("SubQueryContainerAllocationEvent fire:" + executionBlockId);
          }


          if(request.isFirst.getAndSet(false)){
            queryTaskContext.getEventHandler().handle(new SubQueryContainerAllocationEvent(executionBlockId, tasksLaunchMap));
          }
          launchTaskRunners(request.event, tasksLaunchMap);
        }
      }
    }
  }

  class ResourceDeAllocator extends Thread {
    private AtomicBoolean stop = new AtomicBoolean(false);
    final BlockingQueue<TajoMasterProtocol.AllocatedWorkerResourceProto> queue = Queues.newLinkedBlockingQueue();
    private QueryId queryId;

    public void startDeAllocator(QueryId queryId) {
      this.queryId = queryId;
    }

    public void releseResource(TajoMasterProtocol.AllocatedWorkerResourceProto resourceProto) {
      queue.add(resourceProto);
    }

    public void releseResources(Collection<TajoMasterProtocol.AllocatedWorkerResourceProto> resourceProtos) {
      queue.addAll(resourceProtos);
    }

    public synchronized void shutdown() {
      if (stop.getAndSet(true)) {
        return;
      }
    }

    @Override
    public void run() {
      while (!stop.get() && !Thread.currentThread().isInterrupted()) {
        List<TajoMasterProtocol.AllocatedWorkerResourceProto> targets = Lists.newArrayList();
        Queues.drainUninterruptibly(queue, targets, workerInfoMap.size(), 50, TimeUnit.MILLISECONDS);

        if (targets.size() > 0) {
          releaseWorkerResourceRequest(queryId, targets);
        }
      }
    }
  }
}
