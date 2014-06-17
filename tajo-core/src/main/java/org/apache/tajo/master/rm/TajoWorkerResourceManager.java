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

package org.apache.tajo.master.rm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.RpcCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.rpc.CallFuture;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.ipc.TajoMasterProtocol.*;


/**
 * It manages all resources of tajo workers.
 */
public class TajoWorkerResourceManager extends CompositeService implements WorkerResourceManager {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(TajoWorkerResourceManager.class);

  private TajoMaster.MasterContext masterContext;

  private TajoRMContext rmContext;

  private String queryIdSeed;

  private WorkerResourceAllocationThread workerResourceAllocator;

  /**
   * Worker Liveliness monitor
   */
  private WorkerLivelinessMonitor workerLivelinessMonitor;

  private BlockingQueue<WorkerResourceRequest> requestQueue;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private TajoConf systemConf;

  /** It receives status messages from workers and their resources. */
  private TajoResourceTracker resourceTracker;

  public TajoWorkerResourceManager(TajoMaster.MasterContext masterContext) {
    super(TajoWorkerResourceManager.class.getSimpleName());
    this.masterContext = masterContext;
  }

  public TajoWorkerResourceManager(TajoConf systemConf) {
    super(TajoWorkerResourceManager.class.getSimpleName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Preconditions.checkArgument(conf instanceof TajoConf);
    this.systemConf = (TajoConf) conf;

    AsyncDispatcher dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    rmContext = new TajoRMContext(dispatcher);

    this.queryIdSeed = String.valueOf(System.currentTimeMillis());

    requestQueue = new LinkedBlockingDeque<WorkerResourceRequest>();

    workerResourceAllocator = new WorkerResourceAllocationThread();
    workerResourceAllocator.start();

    this.workerLivelinessMonitor = new WorkerLivelinessMonitor(this.rmContext.getDispatcher());
    addIfService(this.workerLivelinessMonitor);

    // Register event handler for Workers
    rmContext.getDispatcher().register(WorkerEventType.class, new WorkerEventDispatcher(rmContext));

    resourceTracker = new TajoResourceTracker(rmContext, workerLivelinessMonitor);
    addIfService(resourceTracker);

    super.serviceInit(systemConf);
  }

  @InterfaceAudience.Private
  public static final class WorkerEventDispatcher implements EventHandler<WorkerEvent> {

    private final TajoRMContext rmContext;

    public WorkerEventDispatcher(TajoRMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(WorkerEvent event) {
      int workerId = event.getWorkerId();
      Worker node = this.rmContext.getWorkers().get(workerId);
      if (node != null) {
        try {
          node.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType() + " for node " + workerId, t);
        }
      }
    }
  }

  @Override
  public Map<Integer, Worker> getWorkers() {
    return ImmutableMap.copyOf(rmContext.getWorkers());
  }

  @Override
  public Map<Integer, Worker> getInactiveWorkers() {
    return ImmutableMap.copyOf(rmContext.getInactiveWorkers());
  }

  public Collection<Integer> getQueryMasterWorkerIds() {
    return Collections.unmodifiableSet(rmContext.getQueryMasterWorker());
  }

  @Override
  public TajoMasterProtocol.ClusterResourceSummary getClusterResourceSummary() {
    return resourceTracker.getClusterResourceSummary();
  }

  @Override
  public void serviceStop() throws Exception {
    if(stopped.get()) {
      return;
    }
    stopped.set(true);
    if(workerResourceAllocator != null) {
      workerResourceAllocator.interrupt();
    }

    super.serviceStop();
  }

  /**
   *
   * @return The prefix of queryId. It is generated when a TajoMaster starts up.
   */
  @Override
  public String getSeedQueryId() throws IOException {
    return queryIdSeed;
  }

  @VisibleForTesting
  TajoResourceTracker getResourceTracker() {
    return resourceTracker;
  }

  private WorkerResourcesRequestProto createQMResourceRequest() {
    float queryMasterDefaultDiskSlot = masterContext.getConf().getFloatVar(
        TajoConf.ConfVars.TAJO_QUERYMASTER_DISK_SLOT);
    int queryMasterDefaultMemoryMB = masterContext.getConf().getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MEMORY_MB);

    WorkerResourcesRequestProto.Builder builder = WorkerResourcesRequestProto.newBuilder();
    builder.setMaxMemoryMBPerContainer(queryMasterDefaultMemoryMB);
    builder.setMinMemoryMBPerContainer(queryMasterDefaultMemoryMB);
    builder.setMaxDiskSlotPerContainer(queryMasterDefaultDiskSlot);
    builder.setMinDiskSlotPerContainer(queryMasterDefaultDiskSlot);
    builder.setResourceRequestPriority(TajoMasterProtocol.ResourceRequestPriority.MEMORY);
    builder.setNumContainers(1);
    return builder.build();
  }

  @Override
  public AllocatedWorkerResourceProto allocateQueryMaster(QueryInProgress queryInProgress) {
    // Create a resource request for a query master
    WorkerResourcesRequestProto qmResourceRequest = createQMResourceRequest();

    // call future for async call
    CallFuture<WorkerResourceAllocationResponse> callFuture = new CallFuture<WorkerResourceAllocationResponse>();
    allocateWorkerResources(qmResourceRequest, callFuture);

    // Wait for 3 seconds
    WorkerResourceAllocationResponse response = null;
    try {
      response = callFuture.get(3, TimeUnit.SECONDS);
    } catch (Throwable t) {
      LOG.error(t);
      return null;
    }

    if (response.getAllocatedWorkerResourceCount() == 0) {
      return null;
    }

    AllocatedWorkerResourceProto resource = response.getAllocatedWorkerResource(0);
    registerQueryMaster(queryInProgress.getQueryId(), resource);
    return resource;
  }

  private void registerQueryMaster(QueryId queryId, AllocatedWorkerResourceProto resource) {
    rmContext.getQueryMasterResource().putIfAbsent(queryId, resource);
  }

  @Override
  public void allocateWorkerResources(WorkerResourcesRequestProto request,
                                      RpcCallback<WorkerResourceAllocationResponse> callBack) {
    try {
      //TODO checking queue size
      requestQueue.put(new WorkerResourceRequest(false, request, callBack));
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  static class WorkerResourceRequest {
    boolean queryMasterRequest;
    WorkerResourcesRequestProto request;
    RpcCallback<WorkerResourceAllocationResponse> callBack;
    WorkerResourceRequest(
        boolean queryMasterRequest, WorkerResourcesRequestProto request,
        RpcCallback<WorkerResourceAllocationResponse> callBack) {
      this.queryMasterRequest = queryMasterRequest;
      this.request = request;
      this.callBack = callBack;
    }
  }

  class WorkerResourceAllocationThread extends Thread {
    @Override
    public void run() {
      LOG.info("WorkerResourceAllocationThread start");
      while(!stopped.get()) {
        try {
          WorkerResourceRequest resourceRequest = requestQueue.take();

          if (LOG.isDebugEnabled()) {
            LOG.debug("allocateWorkerResources:" +
                " requiredMemory:" + resourceRequest.request.getMinMemoryMBPerContainer() +
                "~" + resourceRequest.request.getMaxMemoryMBPerContainer() +
                ", requiredContainers:" + resourceRequest.request.getNumContainers() +
                ", requiredDiskSlots:" + resourceRequest.request.getMinDiskSlotPerContainer() +
                "~" + resourceRequest.request.getMaxDiskSlotPerContainer() +
                ", queryMasterRequest=" + resourceRequest.queryMasterRequest +
                ", liveWorkers=" + rmContext.getWorkers().size());
          }

          List<AllocatedWorkerResourceProto> allocatedWorkerResources = chooseWorkers(resourceRequest);

          if (allocatedWorkerResources.size() > 0) {
            resourceRequest.callBack.run(WorkerResourceAllocationResponse.newBuilder()
                    .addAllAllocatedWorkerResource(allocatedWorkerResources)
                    .build()
            );

          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("=========================================");
              LOG.debug("Available Workers");
              for (Integer liveWorker : rmContext.getWorkers().keySet()) {
                LOG.debug(rmContext.getWorkers().get(liveWorker).toString());
              }
              LOG.debug("=========================================");
            }
            requestQueue.put(resourceRequest);
            Thread.sleep(50);
          }

        } catch (InterruptedException ie) {
          LOG.error(ie);
        }
      }
    }
  }

  private List<AllocatedWorkerResourceProto> chooseWorkers(WorkerResourceRequest resourceRequest) {
    List<AllocatedWorkerResourceProto> selectedWorkers = new ArrayList<AllocatedWorkerResourceProto>();

    int allocatedResources = 0;
    WorkerResourcesRequestProto request = resourceRequest.request;
    AllocatedWorkerResourceProto.Builder allocatedResourceBuilder = AllocatedWorkerResourceProto.newBuilder();
    TajoMasterProtocol.ResourceRequestPriority resourceRequestPriority
        = request.getResourceRequestPriority();

    List<Integer> randomWorkers = new ArrayList<Integer>();

    if(request.getWorkerIdCount() > 0) {
      for (int workerId : request.getWorkerIdList()){
        if(rmContext.getWorkers().containsKey(workerId)){
          randomWorkers.add(workerId);
        } else {
          LOG.warn("Can't found worker : " + workerId);
        }
      }

      if(request.getNumContainers() > randomWorkers.size()){
        ArrayList<Integer> workerIds = new ArrayList<Integer>(rmContext.getWorkers().keySet());
        Collections.shuffle(workerIds);
        for (int workerId : workerIds){
          if (!randomWorkers.contains(workerId)) {
            randomWorkers.add(workerId);
          }

          if (randomWorkers.size() == request.getNumContainers()) {
            break;
          }
        }
      }
    } else {
      randomWorkers.addAll(rmContext.getWorkers().keySet());
      Collections.shuffle(randomWorkers);
    }

    if(resourceRequestPriority == TajoMasterProtocol.ResourceRequestPriority.MEMORY) {
      synchronized(rmContext) {
        int numContainers = resourceRequest.request.getNumContainers();
        int minMemoryMB = resourceRequest.request.getMinMemoryMBPerContainer();
        int maxMemoryMB = resourceRequest.request.getMaxMemoryMBPerContainer();
        float diskSlot = Math.max(resourceRequest.request.getMaxDiskSlotPerContainer(),
            resourceRequest.request.getMinDiskSlotPerContainer());

        int liveWorkerSize = randomWorkers.size();
        Set<Integer> insufficientWorkers = new HashSet<Integer>();
        boolean stop = false;
        boolean checkMax = true;
        while(!stop) {
          if(allocatedResources >= numContainers) {
            break;
          }

          if(insufficientWorkers.size() >= liveWorkerSize) {
            if(!checkMax) {
              break;
            }
            insufficientWorkers.clear();
            checkMax = false;
          }
          int compareAvailableMemory = checkMax ? maxMemoryMB : minMemoryMB;

          for(Integer eachWorker: randomWorkers) {
            if(allocatedResources >= numContainers) {
              stop = true;
              break;
            }

            if(insufficientWorkers.size() >= liveWorkerSize) {
              break;
            }

            Worker worker = rmContext.getWorkers().get(eachWorker);
            WorkerResource workerResource = worker.getResource();
            if(workerResource.getAvailableMemoryMB() >= compareAvailableMemory) {
              allocatedResourceBuilder.setWorker(worker.createWorkerResourceProto());

              int workerMemory;
              if(workerResource.getAvailableMemoryMB() >= maxMemoryMB) {
                workerMemory = maxMemoryMB;
              } else {
                workerMemory = workerResource.getAvailableMemoryMB();
              }
              allocatedResourceBuilder.setAllocatedMemoryMB(workerMemory);

              float workerDiskSlots;
              if(workerResource.getAvailableDiskSlots() >= diskSlot) {
                workerDiskSlots = diskSlot;
              } else {
                workerDiskSlots = workerResource.getAvailableDiskSlots();
              }
              allocatedResourceBuilder.setAllocatedDiskSlots(workerDiskSlots);

              workerResource.allocateResource(workerDiskSlots, workerMemory);
              selectedWorkers.add(allocatedResourceBuilder.build());
              allocatedResources++;

            } else {
              insufficientWorkers.add(eachWorker);
            }
          }
        }
      }
    } else {
      synchronized(rmContext) {
        int numContainers = resourceRequest.request.getNumContainers();
        float minDiskSlots = resourceRequest.request.getMinDiskSlotPerContainer();
        float maxDiskSlots = resourceRequest.request.getMaxDiskSlotPerContainer();
        int memoryMB = Math.max(resourceRequest.request.getMaxMemoryMBPerContainer(),
            resourceRequest.request.getMinMemoryMBPerContainer());

        int liveWorkerSize = randomWorkers.size();
        Set<Integer> insufficientWorkers = new HashSet<Integer>();
        boolean stop = false;
        boolean checkMax = true;
        while(!stop) {
          if(allocatedResources >= numContainers) {
            break;
          }

          if(insufficientWorkers.size() >= liveWorkerSize) {
            if(!checkMax) {
              break;
            }
            insufficientWorkers.clear();
            checkMax = false;
          }
          float compareAvailableDisk = checkMax ? maxDiskSlots : minDiskSlots;

          for(Integer eachWorker: randomWorkers) {
            if(allocatedResources >= numContainers) {
              stop = true;
              break;
            }

            if(insufficientWorkers.size() >= liveWorkerSize) {
              break;
            }

            Worker worker = rmContext.getWorkers().get(eachWorker);
            WorkerResource workerResource = worker.getResource();

            if(workerResource.getAvailableDiskSlots() >= compareAvailableDisk) {
              allocatedResourceBuilder.setWorker(worker.createWorkerResourceProto());

              float workerDiskSlots;
              if(workerResource.getAvailableDiskSlots() >= maxDiskSlots) {
                workerDiskSlots = maxDiskSlots;
              } else {
                workerDiskSlots = workerResource.getAvailableDiskSlots();
              }
              allocatedResourceBuilder.setAllocatedDiskSlots(workerDiskSlots);

              int workerMemory;
              if(workerResource.getAvailableMemoryMB() >= memoryMB) {
                workerMemory = memoryMB;
              } else {
                workerMemory = workerResource.getAvailableMemoryMB();
              }

              if(workerMemory == 0) {
                insufficientWorkers.add(eachWorker);
                continue;
              }

              allocatedResourceBuilder.setAllocatedMemoryMB(workerMemory);

              workerResource.allocateResource(workerDiskSlots, workerMemory);
              selectedWorkers.add(allocatedResourceBuilder.build());
              allocatedResources++;

            } else {
              insufficientWorkers.add(eachWorker);
            }
          }
        }
      }
    }
    return selectedWorkers;
  }

  /**
   * Release allocated resource.
   *
   * @param resource AllocatedWorkerResourceProto to be released
   */
  @Override
  public void releaseWorkerResource(AllocatedWorkerResourceProto resource) {
//    AllocatedWorkerResource allocated = allocatedResourceMap.get(containerId);
    WorkerResourceProto workerResource = resource.getWorker();
    int workerId = Worker.getWorkerId(workerResource.getHost(), workerResource.getPeerRpcPort());

    Worker worker = rmContext.getWorkers().get(workerId);
    if(worker != null) {
      LOG.debug("Release Resource: " + resource.getAllocatedDiskSlots() + "," + resource.getAllocatedMemoryMB());
      worker.getResource().releaseResource(resource.getAllocatedDiskSlots(), resource.getAllocatedMemoryMB());
    } else {
      LOG.warn("No AllocatedWorkerResource data for [" + resource.getWorker() + "]");
      return;
    }
  }

  @Override
  public boolean isQueryMasterStopped(QueryId queryId) {
    return !rmContext.getQueryMasterResource().containsKey(queryId);
  }

  @Override
  public void stopQueryMaster(QueryId queryId) {
    if(!rmContext.getQueryMasterResource().containsKey(queryId)) {
      LOG.warn("No QueryMaster resource info for " + queryId);
      return;
    } else {
      AllocatedWorkerResourceProto resource = rmContext.getQueryMasterResource().remove(queryId);
      releaseWorkerResource(resource);
//      rmContext.getStoppedQueryIds().add(queryId);
      LOG.info(String.format("Released QueryMaster (%s) resource:" + resource.getWorker().getHost(), queryId.toString()));
    }
  }

  public TajoRMContext getRMContext() {
    return rmContext;
  }
}
