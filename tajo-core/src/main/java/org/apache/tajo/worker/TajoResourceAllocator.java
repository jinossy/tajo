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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.ContainerProxy;
import org.apache.tajo.master.TajoContainerProxy;
import org.apache.tajo.master.TaskRunnerGroupEvent;
import org.apache.tajo.master.TaskRunnerLauncher;
import org.apache.tajo.master.event.ContainerAllocationEvent;
import org.apache.tajo.master.event.ContainerAllocatorEventType;
import org.apache.tajo.master.event.ContainerRequestEvent;
import org.apache.tajo.master.event.SubQueryContainerAllocationEvent;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.master.querymaster.SubQueryState;
import org.apache.tajo.master.rm.TajoWorkerContainer;
import org.apache.tajo.master.rm.TajoWorkerContainerId;
import org.apache.tajo.master.rm.Worker;
import org.apache.tajo.master.rm.WorkerResource;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.scheduler.MultiQueueFiFoScheduler;
import org.apache.tajo.scheduler.Scheduler;
import org.apache.tajo.util.ApplicationIdUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ipc.TajoMasterProtocol.WorkerResourceAllocationResponse;

public class TajoResourceAllocator extends AbstractResourceAllocator {
  private static final Log LOG = LogFactory.getLog(TajoResourceAllocator.class);

  private TajoConf tajoConf;
  private QueryMasterTask.QueryMasterTaskContext queryTaskContext;
  private final ExecutorService executorService;
  private ConcurrentHashMap<ContainerId, TajoMasterProtocol.AllocatedWorkerResourceProto>
      resourceMap = new ConcurrentHashMap<ContainerId, TajoMasterProtocol.AllocatedWorkerResourceProto>();

  /**
   * A key is containerId, and a value is a worker id.
   */
  private Map<ContainerId, Integer> workerMap = Maps.newConcurrentMap();
  private AtomicInteger reservedContainer = new AtomicInteger(0); //TODO handle from scheduler
  private ContainerAllocator allocatorThread;

  public TajoResourceAllocator(QueryMasterTask.QueryMasterTaskContext queryTaskContext) {
    this.queryTaskContext = queryTaskContext;
    executorService = Executors.newFixedThreadPool(
        queryTaskContext.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM));
  }

  @Override
  public ContainerId makeContainerId(YarnProtos.ContainerIdProto containerIdProto) {
    return new TajoWorkerContainerId(containerIdProto);
  }

  /* sync call */
  @Override
  public boolean reserveContainer(ExecutionBlockId ebId, ContainerId containerId) {
    if (isInState(STATE.STOPPED) || !isRunningState(ebId)) return false;

    if(resourceMap.containsKey(containerId)){
      return true;
    } else {
      boolean isLeaf = queryTaskContext.getSubQuery(ebId).getMasterPlan().isLeaf(ebId);

      List<Integer> workerIds = Lists.newArrayList();
      workerIds.add(workerMap.get(containerId));
      if(LOG.isDebugEnabled()){
        LOG.debug("reserve container : " + ebId + "," + containerId + "," + workerMap.get(containerId));
      }

      WorkerResourceAllocationResponse response = reserveWokerResources(workerIds.size(), isLeaf, workerIds);
      if (response != null && response.getAllocatedWorkerResourceCount() > 0) {
        List<TajoMasterProtocol.AllocatedWorkerResourceProto> allocatedResources = response.getAllocatedWorkerResourceList();

        if(isRunningState(ebId)){
          resourceMap.put(containerId, allocatedResources.get(0));
          return true;
        } else {
          LOG.warn("ExecutionBlock is not running state : " + ebId);
          releaseWorkerResources(allocatedResources);
        }
      }
    }
    return false;
  }

  @Override
  public void releaseContainer(ContainerId containerId) {
    ContainerProxy proxy = getContainer(containerId);
    if (proxy != null) {
      executorService.submit(new ContainerExecutor(this, proxy, ContainerRequestEvent.EventType.CONTAINER_RELEASE));
    }
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

  @Override
  public void init(Configuration conf) {
    tajoConf = (TajoConf) conf;

    queryTaskContext.getDispatcher().register(TaskRunnerGroupEvent.EventType.class, new TajoTaskRunnerLauncher());

    queryTaskContext.getDispatcher().register(ContainerAllocatorEventType.class, new TajoWorkerAllocationHandler());
    queryTaskContext.getDispatcher().register(ContainerRequestEvent.EventType.class, new ContainerHandler());

    super.init(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    allocatorThread.shutdown();

    Map<ContainerId, ContainerProxy> containers = queryTaskContext.getResourceAllocator().getContainers();
    List<ContainerProxy> list = new ArrayList<ContainerProxy>(containers.values());
    for (ContainerProxy eachProxy : list) {
      try {
        eachProxy.stopContainer();
      } catch (Exception e) {
        LOG.warn(e.getMessage());
      }
    }

    LOG.info("Release remaining Resources" + resourceMap);
    releaseWorkerResources(Lists.newArrayList(resourceMap.values()));
    resourceMap.clear();
    executorService.shutdown();
  }

  @Override
  public void start() {
    super.start();
    allocatorThread = new ContainerAllocator(this);
    allocatorThread.start();
  }

  private boolean isRunningState(ExecutionBlockId ebId){
    SubQuery subQuery = queryTaskContext.getSubQuery(ebId);
    return subQuery != null && SubQuery.isRunningState(subQuery.getState());
  }

  class TajoTaskRunnerLauncher implements TaskRunnerLauncher {
    @Override
    public void handle(TaskRunnerGroupEvent event) {
      if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_LAUNCH) {
        launchTaskRunners(event.getExecutionBlockId(), event.getContainers());
      } else if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_CLEANUP) {
        allocatorThread.stopCurrentContainerAllocator();
        stopContainers(event.getContainers());
      }
    }
  }

  private void launchTaskRunners(ExecutionBlockId executionBlockId, Collection<Container> containers) {
    // Query in standby mode doesn't need launch Worker.
    // But, Assign ExecutionBlock to assigned tajo worker
    for (Container eachContainer : containers) {
      TajoContainerProxy containerProxy = new TajoContainerProxy(queryTaskContext, tajoConf,
          eachContainer, executionBlockId);
      executorService.submit(new ContainerExecutor(this, containerProxy, ContainerAllocatorEventType.CONTAINER_LAUNCH));
    }
  }

  private void stopContainers(Collection<Container> containers) {
    for (Container container : containers) {
      final ContainerProxy proxy = queryTaskContext.getResourceAllocator().getContainer(container.getId());
      executorService.submit(new ContainerExecutor(this, proxy, ContainerAllocatorEventType.CONTAINER_DEALLOCATE));
      reservedContainer.decrementAndGet();
    }
  }

  private synchronized void releaseWorkerResources(List<TajoMasterProtocol.AllocatedWorkerResourceProto> resources) {
    if (resources.size() == 0) return;

    RpcConnectionPool connPool = RpcConnectionPool.getPool(queryTaskContext.getConf());
    NettyClientBase tmClient = null;
    try {
      tmClient = connPool.getConnection(queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
          TajoMasterProtocol.class, true);
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();
      masterClientService.releaseWorkerResource(null,
          TajoMasterProtocol.WorkerResourceReleaseProto.newBuilder()
              .addAllResources(resources)
              .build(),
          NullCallback.get()
      );
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      connPool.releaseConnection(tmClient);
    }
  }

  private void releaseWorkerResource(ContainerId containerId) {
    if (resourceMap.containsKey(containerId)) {
      TajoMasterProtocol.AllocatedWorkerResourceProto allocatedWorkerResource = resourceMap.remove(containerId);
      if (LOG.isInfoEnabled()) {
        ContainerProxy proxy = getContainer(containerId);
        LOG.debug("Release Worker Resource: " + allocatedWorkerResource + "," + containerId + ", state:" + proxy.getState());
      }
      List<TajoMasterProtocol.AllocatedWorkerResourceProto> resources = new ArrayList<TajoMasterProtocol.AllocatedWorkerResourceProto>();
      resources.add(allocatedWorkerResource);
      releaseWorkerResources(resources);
    }
  }

  private static class ContainerExecutor implements Runnable {
    private final ContainerProxy proxy;
    private final Enum<?> eventType;
    private TajoResourceAllocator allocator;

    public ContainerExecutor(TajoResourceAllocator allocator, ContainerProxy proxy, Enum<?> eventType) {
      this.proxy = proxy;
      this.eventType = eventType;
      this.allocator = allocator;
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ContainerProxy " + eventType + " : " + proxy.getContainerId() + "," + proxy.getId());
      }

      if (eventType == ContainerAllocatorEventType.CONTAINER_LAUNCH) {
        proxy.launch(null);
      } else if (eventType == ContainerRequestEvent.EventType.CONTAINER_RELEASE) {
        try {
          allocator.releaseWorkerResource(proxy.getContainerId());
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      } else if (eventType == ContainerAllocatorEventType.CONTAINER_DEALLOCATE)
        proxy.stopContainer();
    }
  }

  class ContainerHandler implements EventHandler<ContainerRequestEvent> {
    @Override
    public void handle(ContainerRequestEvent event) {
      if (event.getType() == ContainerRequestEvent.EventType.CONTAINER_RELEASE) {
        releaseContainer(event.getContainerId());
      } else if (event.getType() == ContainerRequestEvent.EventType.CONTAINER_RESERVE) {
        reserveContainer(event.getExecutionBlockId(), event.getContainerId());
      }
    }
  }

  class TajoWorkerAllocationHandler implements EventHandler<ContainerAllocationEvent> {

    @Override
    public void handle(ContainerAllocationEvent event) {
      allocatorThread.startContainerAllocation(event);
    }
  }

  private TajoMasterProtocol.WorkerResourcesRequestProto createWorkerResourcesRequest(int wokerResource,
                                                                                      TajoMasterProtocol.ResourceRequestPriority requestPriority,
                                                                                      List<Integer> workerIds) {
    //TODO consider task's resource usage pattern
    int requiredMemoryMB = tajoConf.getIntVar(TajoConf.ConfVars.TASK_DEFAULT_MEMORY);
    float requiredDiskSlots = tajoConf.getFloatVar(TajoConf.ConfVars.TASK_DEFAULT_DISK);

    return TajoMasterProtocol.WorkerResourcesRequestProto.newBuilder()
        .setMinMemoryMBPerContainer(requiredMemoryMB)
        .setMaxMemoryMBPerContainer(requiredMemoryMB)
        .setNumContainers(wokerResource)
        .setResourceRequestPriority(requestPriority)
        .setMinDiskSlotPerContainer(requiredDiskSlots)
        .setMaxDiskSlotPerContainer(requiredDiskSlots)
        .addAllWorkerId(workerIds)
        .build();
  }

  private WorkerResourceAllocationResponse reserveWokerResources(int required,
                                                                 boolean isLeaf,
                                                                 List<Integer> workerIds) {
    TajoMasterProtocol.ResourceRequestPriority priority =
        isLeaf ? TajoMasterProtocol.ResourceRequestPriority.DISK : TajoMasterProtocol.ResourceRequestPriority.MEMORY;

    CallFuture<WorkerResourceAllocationResponse> callBack =
        new CallFuture<WorkerResourceAllocationResponse>();

    RpcConnectionPool connPool = RpcConnectionPool.getPool(queryTaskContext.getConf());
    NettyClientBase tmClient = null;
    try {
      tmClient = connPool.getConnection(
          queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
          TajoMasterProtocol.class, true);
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();
      masterClientService.allocateWorkerResources(null, createWorkerResourcesRequest(required, priority, workerIds), callBack);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      connPool.releaseConnection(tmClient);
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
        LOG.info("No available worker resource for " + queryTaskContext.getQueryId() + "," + resourceMap);

        continue;
      }
    }
    return response;
  }

  class ContainerAllocator extends Thread {
    AtomicInteger containerIdSeq = new AtomicInteger(0);
    private AtomicBoolean stop = new AtomicBoolean(false);
    final TajoResourceAllocator allocator;
    final Map<String, MultiQueueFiFoScheduler.QueueProperty> queuePropertyMap;
    final BlockingDeque<ContainerAllocationEvent> eventQueue =
        new LinkedBlockingDeque<ContainerAllocationEvent>();

    public ContainerAllocator(TajoResourceAllocator allocator) {
      this.allocator = allocator;
      this.queuePropertyMap = Maps.newHashMap();
      List<MultiQueueFiFoScheduler.QueueProperty> queueProperties = MultiQueueFiFoScheduler.loadQueueProperty(tajoConf);
      for (MultiQueueFiFoScheduler.QueueProperty queueProperty : queueProperties) {
        queuePropertyMap.put(queueProperty.getQueueName(), queueProperty);
      }
    }

    public synchronized void startContainerAllocation(ContainerAllocationEvent event) {
      try {
        LOG.info("Start allocation. containers(" + event.getRequiredNum() + ") executionBlockId : " + event.getExecutionBlockId());
        eventQueue.put(event);
      } catch (InterruptedException e) {
        if (!stop.get()) {
          LOG.warn("ContainerAllocator thread interrupted");
        }
      }
    }

    public synchronized void stopCurrentContainerAllocator() {
      if (eventQueue.size() > 0) {
        ContainerAllocationEvent event = eventQueue.removeFirst();   //TODO check the ebid
        LOG.warn("Container allocator force stopped. executionBlockId : " + event.getExecutionBlockId());
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

      if (allocatorThread != null) {
        allocatorThread.interrupt();
      }
    }

    private ContainerId createContainerId(ExecutionBlockId executionBlockId) {
      ApplicationAttemptId applicationAttemptId = ApplicationIdUtils.createApplicationAttemptId(executionBlockId.getQueryId());
      return new TajoWorkerContainerId(applicationAttemptId, containerIdSeq.incrementAndGet());
    }

    @Override
    public void run() {
      while (!stop.get() && !Thread.currentThread().isInterrupted()) {
        ContainerAllocationEvent event;
        try {
          event = eventQueue.take();
        } catch (InterruptedException ie) {
          if (!stop.get()) {
            LOG.warn("ContainerAllocator thread interrupted");
          }
          break;
        }
        ExecutionBlockId executionBlockId = event.getExecutionBlockId();
        SubQueryState state = queryTaskContext.getSubQuery(executionBlockId).getState();
        MultiQueueFiFoScheduler.QueueProperty queueProperty =
            queuePropertyMap.get(queryTaskContext.getSession().getVariable(Scheduler.QUERY_QUEUE_KEY, Scheduler.DEFAULT_QUEUE_NAME));
        int resources = event.getRequiredNum();
        if (queueProperty != null && queueProperty.getMaxCapacity() > 0) {
          resources = Math.min(event.getRequiredNum(), queueProperty.getMaxCapacity());
        }
        if (SubQuery.isRunningState(state)) {
          allocateContainers(executionBlockId, event.isLeafQuery(), resources);
        }


        if (resources <= allocator.reservedContainer.get()) {
          LOG.info("All containers(" + allocator.reservedContainer.get() + ") Allocated. executionBlockId : "
              + event.getExecutionBlockId());
          continue;
        }

        try {
          synchronized (allocatorThread) {
            allocatorThread.wait(100);
          }

          state = queryTaskContext.getSubQuery(executionBlockId).getState();
          if (!SubQuery.isRunningState(state)) {
            LOG.warn("ExecutionBlock is not running state : " + state + ", " + executionBlockId);
            releaseWorkerResources(Lists.newArrayList(resourceMap.values()));
            resourceMap.clear();
          } else {
            eventQueue.addFirst(event);
            if(LOG.isDebugEnabled()){
              LOG.debug("Retry to allocate containers executionBlockId : " + event.getExecutionBlockId());
            }
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

    private void allocateContainers(ExecutionBlockId executionBlockId, boolean isLeaf, int resources) {
      if(LOG.isDebugEnabled()){
        LOG.debug("Try to allocate containers executionBlockId : " + executionBlockId + "," + resources);
      }

      WorkerResourceAllocationResponse response =
          reserveWokerResources(resources - reservedContainer.get(), isLeaf, new ArrayList<Integer>());

      if (response != null) {
        List<TajoMasterProtocol.AllocatedWorkerResourceProto> allocatedResources = response.getAllocatedWorkerResourceList();

        List<Container> containers = new ArrayList<Container>();
        for (TajoMasterProtocol.AllocatedWorkerResourceProto eachAllocatedResource : allocatedResources) {

          TajoWorkerContainer container = new TajoWorkerContainer();
          NodeId nodeId = NodeId.newInstance(eachAllocatedResource.getWorker().getHost(),
              eachAllocatedResource.getWorker().getPeerRpcPort());

          container.setId(createContainerId(executionBlockId));
          container.setNodeId(nodeId);
          resourceMap.put(container.getId(), eachAllocatedResource);

          WorkerResource workerResource = new WorkerResource();
          workerResource.setMemoryMB(eachAllocatedResource.getAllocatedMemoryMB());
          workerResource.setDiskSlots(eachAllocatedResource.getAllocatedDiskSlots());

          Worker worker = new Worker(null, workerResource);
          worker.setHostName(nodeId.getHost());
          worker.setPeerRpcPort(nodeId.getPort());
          worker.setQueryMasterPort(eachAllocatedResource.getWorker().getQueryMasterPort());
          worker.setPullServerPort(eachAllocatedResource.getWorker().getPullServerPort());

          container.setWorkerResource(worker);

          containers.add(container);
          workerMap.put(container.getId(), container.getWorkerResource().getWorkerId());
        }

        if (allocatedResources.size() > 0) {
          reservedContainer.addAndGet(allocatedResources.size());
          LOG.info("Reserved worker resources : " + allocatedResources.size() + "/" + reservedContainer.get()
              + ", EBId : " + executionBlockId);
          LOG.debug("SubQueryContainerAllocationEvent fire:" + executionBlockId);

          if (LOG.isDebugEnabled()) {
            LOG.debug("SubQueryContainerAllocationEvent fire:" + executionBlockId);
          }
          LOG.debug("created containers" + resourceMap);
          queryTaskContext.getEventHandler().handle(new SubQueryContainerAllocationEvent(executionBlockId, containers));
        }
      }
    }
  }
}
