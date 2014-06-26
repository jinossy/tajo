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
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

public abstract class AbstractResourceAllocator extends CompositeService implements ResourceAllocator {
  /**
   * A key is worker id, and a value is a worker connection information.
   */
  protected ConcurrentMap<Integer, WorkerConnectionInfo> workerInfoMap = Maps.newConcurrentMap();

  protected ConcurrentMap<ContainerId, WorkerConnectionInfo> containerIds = Maps.newConcurrentMap();

  public AbstractResourceAllocator() {
    super(AbstractResourceAllocator.class.getName());
  }

  public WorkerConnectionInfo getWorkerConnectionInfo(int workerId) {
    return workerInfoMap.get(workerId);
  }

  public WorkerConnectionInfo getWorkerConnectionInfo(ContainerId containerId) {
    return containerIds.get(containerId);
  }

  protected void addWorkerConnectionInfo(WorkerConnectionInfo connectionInfo) {
    workerInfoMap.putIfAbsent(connectionInfo.getId(), connectionInfo);
  }

  public void addContainerId(ContainerId cId, int workerId) {
    containerIds.putIfAbsent(cId, getWorkerConnectionInfo(workerId));
  }

  public void removeContainer(ContainerId cId) {
    containerIds.remove(cId);
  }

  public boolean containsContainer(ContainerId cId) {
    return containerIds.containsKey(cId);
  }

  public Collection<ContainerId> getContainerIds() {
    return containerIds.keySet();
  }

  @Override
  protected void serviceStop() throws Exception {
    workerInfoMap.clear();
    containerIds.clear();
    super.serviceStop();
  }
}
