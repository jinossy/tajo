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

package org.apache.tajo.worker.rm;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.ipc.TajoMasterProtocol;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Describe current resources of a worker.
 *
 * It includes various resource capability of a worker as follows:
 * <ul>
 *   <li>used/total disk slots</li>
 *   <li>used/total core slots</li>
 *   <li>used/total memory</li>
 *   <li>the number of running tasks</li>
 * </ul>
 */
public class WorkerResource {
  private static final Log LOG = LogFactory.getLog(WorkerResource.class);

  private double diskSlots;
  private int cpuCoreSlots;
  private int memoryMB;

  private final AtomicDouble usedDiskSlots = new AtomicDouble(0D);
  private final AtomicInteger usedMemoryMB = new AtomicInteger(0);
  private final AtomicInteger usedCpuCoreSlots = new AtomicInteger(0);
  private final AtomicInteger numQueryMasterTasks = new AtomicInteger(0);
  private final AtomicInteger numRunningTasks = new AtomicInteger(0);

  public WorkerResource(int cpuCoreSlots, int memoryMB, double diskSlots) {
    this.cpuCoreSlots = cpuCoreSlots;
    this.memoryMB = memoryMB;
    this.diskSlots = diskSlots;
  }

  public long getMaxHeap() {
    return Runtime.getRuntime().maxMemory();
  }

  public long getFreeHeap() {
    return Runtime.getRuntime().freeMemory();
  }

  public long getTotalHeap() {
    return Runtime.getRuntime().totalMemory();
  }

  public double getDiskSlots() {
    return diskSlots;
  }

  public int getCpuCoreSlots() {
    return cpuCoreSlots;
  }

  public int getMemoryMB() {
    return memoryMB;
  }

  public double getAvailableDiskSlots() {
    return diskSlots - usedDiskSlots.get();
  }

  public int getAvailableMemoryMB() {
    return memoryMB - usedMemoryMB.get();
  }

  public int getAvailableCpuCoreSlots() {
    return cpuCoreSlots - usedCpuCoreSlots.get();
  }

  public int getUsedMemoryMB() {
    return usedMemoryMB.get();
  }

  public int getUsedCpuCoreSlots() {
    return usedCpuCoreSlots.get();
  }

  public double getUsedDiskSlots() {
    return usedDiskSlots.get();
  }

  public int getNumRunningTasks() {
    return numRunningTasks.get();
  }

  public void setNumRunningTasks(int numRunningTasks) {
    this.numRunningTasks.set(numRunningTasks);
  }

  public int getNumQueryMasterTasks() {
    return numQueryMasterTasks.get();
  }

  public void releaseResource(float diskSlots, int memoryMB, boolean queryMaster) {
    if(usedMemoryMB.addAndGet(-memoryMB) < 0) {
      LOG.warn("Used memory can't be a minus: " + usedMemoryMB);
      usedMemoryMB.set(0);
    }
    if(usedDiskSlots.addAndGet(-diskSlots) < 0) {
      LOG.warn("Used disk slot can't be a minus: " + usedDiskSlots);
      usedDiskSlots.set(0);
    }

    if(queryMaster){
      numQueryMasterTasks.decrementAndGet();
    } else {
      numRunningTasks.decrementAndGet();
    }

    //TODO notify allocator
  }

  public boolean allocateResource(float diskSlots, int memoryMB, boolean queryMaster) {
    if(usedMemoryMB.addAndGet(memoryMB) > this.memoryMB) {
      usedMemoryMB.addAndGet(-memoryMB);
      return false;
    }

    if(usedDiskSlots.addAndGet(diskSlots) > this.diskSlots) {
      usedDiskSlots.addAndGet(-diskSlots);
      usedMemoryMB.addAndGet(-memoryMB);
      return false;
    }

    if(queryMaster){
      numQueryMasterTasks.incrementAndGet();
    } else {
      numRunningTasks.incrementAndGet();
    }
    return true;
  }

  public boolean reserve(TajoMasterProtocol.WorkerResourceProto resourceProto){
    //TODO
    /*
    * 1. add priority queue
    * 2. allocate resource : returned reservation key
    * 3. add reservation blocking queue
    * 4. return callFuture
    * 5. finalize/cancel reservation : remove reservation blocking queue
    * 6. release reservation timeout
    *
    * */
    return true;
  }

  @Override
  public String toString() {
    return "slots=m:" + memoryMB + ",d:" + diskSlots +
        ",c:" + cpuCoreSlots + ", used=m:" + usedMemoryMB + ",d:" + usedDiskSlots + ",c:" + usedCpuCoreSlots;
  }
}
