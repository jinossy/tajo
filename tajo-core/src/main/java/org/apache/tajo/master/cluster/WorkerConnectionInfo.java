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

package org.apache.tajo.master.cluster;

import com.google.common.base.Objects;
import org.apache.tajo.common.ProtoObject;

import static org.apache.tajo.TajoProtos.WorkerConnectionInfoProto;

public class WorkerConnectionInfo implements ProtoObject<WorkerConnectionInfoProto>, Comparable<WorkerConnectionInfo> {
  private WorkerConnectionInfoProto.Builder builder;
  /** Hostname */
  private String host;
  /** Peer rpc port */
  private int peerRpcPort;
  /** pull server port */
  private int pullServerPort;
  /** QueryMaster rpc port */
  private int queryMasterPort;
  /** the port of client rpc which provides an client API */
  private int clientPort;
  /** http info port */
  private int httpInfoPort;

  public WorkerConnectionInfo() {
    builder = WorkerConnectionInfoProto.newBuilder();
  }

  public WorkerConnectionInfo(WorkerConnectionInfoProto proto) {
    this();
    this.host = proto.getHost();
    this.peerRpcPort = proto.getPeerRpcPort();
    this.pullServerPort = proto.getPullServerPort();
    this.clientPort = proto.getClientPort();
    this.httpInfoPort = proto.getHttpInfoPort();
    if (proto.hasQueryMasterPort()) {
      this.queryMasterPort = proto.getQueryMasterPort();
    }
  }

  public WorkerConnectionInfo(String host, int peerRpcPort, int pullServerPort, int clientPort, int httpInfoPort) {
    this();
    this.host = host;
    this.peerRpcPort = peerRpcPort;
    this.pullServerPort = pullServerPort;
    this.clientPort = clientPort;
    this.httpInfoPort = httpInfoPort;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPeerRpcPort() {
    return peerRpcPort;
  }

  public void setPeerRpcPort(int peerRpcPort) {
    this.peerRpcPort = peerRpcPort;
  }

  public int getPullServerPort() {
    return pullServerPort;
  }

  public void setPullServerPort(int pullServerPort) {
    this.pullServerPort = pullServerPort;
  }

  public int getQueryMasterPort() {
    return queryMasterPort;
  }

  public void setQueryMasterPort(int queryMasterPort) {
    this.queryMasterPort = queryMasterPort;
  }

  public int getClientPort() {
    return clientPort;
  }

  public void setClientPort(int clientPort) {
    this.clientPort = clientPort;
  }

  public int getHttpInfoPort() {
    return httpInfoPort;
  }

  public void setHttpInfoPort(int HttpInfoPort) {
    this.httpInfoPort = httpInfoPort;
  }

  public int getId() {
    return hashCode();
  }

  public String getHostAndPeerRpcPort(){
    return this.getHost() + ":" + this.getPeerRpcPort();
  }

  @Override
  public WorkerConnectionInfoProto getProto() {
    builder.setHost(host)
        .setPeerRpcPort(peerRpcPort)
        .setPullServerPort(pullServerPort)
        .setClientPort(clientPort)
        .setHttpInfoPort(httpInfoPort);
    if (queryMasterPort > 0) {
      builder.setQueryMasterPort(queryMasterPort);
    }
    return builder.build();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(host, peerRpcPort);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    WorkerConnectionInfo other = (WorkerConnectionInfo) obj;
    if (!this.getHost().equals(other.getHost()))
      return false;
    if (this.getPeerRpcPort() != other.getPeerRpcPort())
      return false;
    return true;
  }

  @Override
  public int compareTo(WorkerConnectionInfo other) {
    int hostCompare = this.getHost().compareTo(other.getHost());
    if (hostCompare == 0) {
      if (this.getPeerRpcPort() > other.getPeerRpcPort()) {
        return 1;
      } else if (this.getPeerRpcPort() < other.getPeerRpcPort()) {
        return -1;
      }
      return 0;
    }
    return hostCompare;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("host:").append(host).append(", ")
        .append("PeerRpcPort:").append(peerRpcPort).append(", ")
        .append("PullServerPort:").append(pullServerPort).append(", ")
        .append("ClientPort:").append(clientPort).append(", ")
        .append("HttpInfoPort:").append(httpInfoPort).append(", ")
        .append("QueryMasterPort:").append(queryMasterPort);
    return builder.toString();
  }
}