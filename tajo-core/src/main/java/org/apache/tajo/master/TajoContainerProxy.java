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

package org.apache.tajo.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.rm.TajoWorkerContainer;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcConnectionPool;

import java.net.InetSocketAddress;

public class TajoContainerProxy extends ContainerProxy {
  public TajoContainerProxy(QueryMasterTask.QueryMasterTaskContext context,
                            Configuration conf, Container container,
                            ExecutionBlockId executionBlockId) {
    super(context, conf, executionBlockId, container);
  }

  @Override
  public void launch(ContainerLaunchContext containerLaunchContext) {
    this.hostName = container.getNodeId().getHost();
    this.port = ((TajoWorkerContainer)container).getWorkerResource().getConnectionInfo().getPullServerPort();
    this.state = ContainerState.RUNNING;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Launch Container:" + executionBlockId + "," + containerId.getId() + "," +
          container.getId() + "," + container.getNodeId() + ", pullServer=" + port);
    }

    assignExecutionBlock(executionBlockId, container);
  }

  /**
   * It sends a kill RPC request to a corresponding worker.
   *
   * @param taskAttemptId The TaskAttemptId to be killed.
   */
  public void killTaskAttempt(int workerId, QueryUnitAttemptId taskAttemptId) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      InetSocketAddress addr = new InetSocketAddress(container.getNodeId().getHost(), container.getNodeId().getPort());
      tajoWorkerRpc = RpcConnectionPool.getPool(context.getConf()).getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();
      tajoWorkerRpcClient.killTaskAttempt(null, taskAttemptId.getProto(), NullCallback.get());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool(context.getConf()).releaseConnection(tajoWorkerRpc);
    }
  }

  private void assignExecutionBlock(ExecutionBlockId executionBlockId, Container container) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      InetSocketAddress myAddr= context.getQueryMasterContext().getWorkerContext()
          .getQueryMasterManagerService().getBindAddr();

      InetSocketAddress addr = new InetSocketAddress(container.getNodeId().getHost(), container.getNodeId().getPort());
      tajoWorkerRpc = RpcConnectionPool.getPool(context.getConf()).getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();

      TajoWorkerProtocol.RunExecutionBlockRequestProto request =
          TajoWorkerProtocol.RunExecutionBlockRequestProto.newBuilder()
              .setExecutionBlockId(executionBlockId.getProto())
              .setQueryMaster(context.getQueryMasterContext().getWorkerContext().getConnectionInfo().getProto())
              .setTasks(1)
              .setQueryOutputPath(context.getStagingDir().toString())
              .build();

      tajoWorkerRpcClient.executeExecutionBlock(null, request, NullCallback.get());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool(context.getConf()).releaseConnection(tajoWorkerRpc);
    }
  }

  @Override
  public synchronized void stopContainer() {
    if(isCompletelyDone()) {
      LOG.info("Container already stopped:" + containerId);
      return;
    }
    if(this.state == ContainerState.PREP) {
      this.state = ContainerState.KILLED_BEFORE_LAUNCH;
    } else {
      try {
        this.state = ContainerState.DONE;
      } catch (Throwable t) {
        // ignore the cleanup failure
        String message = "cleanup failed for container "
            + this.containerId + " : "
            + StringUtils.stringifyException(t);
        LOG.warn(message);
        this.state = ContainerState.DONE;
        return;
      }
    }
  }
}
