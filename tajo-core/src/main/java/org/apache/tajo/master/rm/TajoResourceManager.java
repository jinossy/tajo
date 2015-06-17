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
import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.scheduler.AbstractQueryScheduler;
import org.apache.tajo.master.scheduler.QuerySchedulingInfo;
import org.apache.tajo.master.scheduler.SimpleScheduler;
import org.apache.tajo.master.scheduler.event.SchedulerEventType;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * It manages all resources of tajo workers.
 */
public class TajoResourceManager extends CompositeService {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(TajoResourceManager.class);

  private TajoMaster.MasterContext masterContext;

  private TajoRMContext rmContext;

  private String queryIdSeed;

  /**
   * Worker Liveliness monitor
   */
  private WorkerLivelinessMonitor workerLivelinessMonitor;

  private TajoConf systemConf;
  private AbstractQueryScheduler scheduler;

  /** It receives status messages from workers and their resources. */
  private TajoResourceTracker resourceTracker;

  public TajoResourceManager(TajoMaster.MasterContext masterContext) {
    super(TajoResourceManager.class.getSimpleName());
    this.masterContext = masterContext;
  }

  @VisibleForTesting
  public TajoResourceManager(TajoConf systemConf) {
    super(TajoResourceManager.class.getSimpleName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);

    AsyncDispatcher dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    rmContext = new TajoRMContext(dispatcher);

    this.queryIdSeed = String.valueOf(System.currentTimeMillis());


    this.workerLivelinessMonitor = new WorkerLivelinessMonitor(this.rmContext.getDispatcher());
    addIfService(this.workerLivelinessMonitor);

    // Register event handler for Workers
    rmContext.getDispatcher().register(WorkerEventType.class, new WorkerEventDispatcher(rmContext));

    resourceTracker = new TajoResourceTracker(this, workerLivelinessMonitor);
    addIfService(resourceTracker);

    //TODO configuable
    scheduler = new SimpleScheduler(masterContext);
    addIfService(scheduler);
    rmContext.getDispatcher().register(SchedulerEventType.class, scheduler);

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

  @Deprecated
  public Map<Integer, Worker> getWorkers() {
    return ImmutableMap.copyOf(rmContext.getWorkers());
  }
  @Deprecated
  public Map<Integer, Worker> getInactiveWorkers() {
    return ImmutableMap.copyOf(rmContext.getInactiveWorkers());
  }
  @Deprecated
  public Collection<Integer> getQueryMasters() {
    return Collections.unmodifiableSet(rmContext.getQueryMasterWorker());
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }

  /**
   *
   * @return The prefix of queryId. It is generated when a TajoMaster starts up.
   */
  public String getSeedQueryId() throws IOException {
    return queryIdSeed;
  }

  @VisibleForTesting
  TajoResourceTracker getResourceTracker() {
    return resourceTracker;
  }

  public AbstractQueryScheduler getScheduler() {
    return scheduler;
  }

  public void submitQuery(QuerySchedulingInfo schedulingInfo) {
    scheduler.submitQuery(schedulingInfo);
  }

  public TajoRMContext getRMContext() {
    return rmContext;
  }
}
