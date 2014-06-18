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

package org.apache.tajo.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.master.querymaster.QueryInProgress;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

public class SimpleFifoScheduler extends AbstractScheduler {
  private static final Log LOG = LogFactory.getLog(SimpleFifoScheduler.class.getName());
  private LinkedList<QuerySchedulingInfo> pool = new LinkedList<QuerySchedulingInfo>();
  private Comparator<QuerySchedulingInfo> COMPARATOR = new SchedulingAlgorithms.FifoComparator();

  public SimpleFifoScheduler() {
  }

  @Override
  public Mode getMode() {
    return Mode.FIFO;
  }

  @Override
  public String getName() {
    return queryJobManager.getName();
  }

  @Override
  public void notifyQueryStop(QueryId queryId) {
    synchronized (pool) {
      pool.remove(getQueryByQueryId(queryId));
    }
    wakeupProcessor();
  }

  private QuerySchedulingInfo getQueryByQueryId(QueryId queryId) {
    for (QuerySchedulingInfo querySchedulingInfo : pool) {
      if (querySchedulingInfo.getQueryId().equals(queryId)) {
        return querySchedulingInfo;
      }
    }
    return null;
  }

  protected void addQueryToQueue(QuerySchedulingInfo querySchedulingInfo) throws Exception {
    synchronized (pool) {
      int qSize = pool.size();
      if (qSize != 0 && qSize % 100 == 0) {
        LOG.info("Size of Fifo queue is " + qSize);
      }

      boolean result = pool.add(querySchedulingInfo);
      if (!result) {
        throw new Exception("Can't add to queue");
      }
    }
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    pool.clear();
  }

  @Override
  protected QuerySchedulingInfo[] getScheduledQueries() {
    synchronized (pool) {
      if (queryJobManager.getRunningQueries().size() == 0) {
        if (pool.size() > 1) {
          Collections.sort(pool, COMPARATOR);
        }
        QuerySchedulingInfo querySchedulingInfo = pool.poll();

        if (querySchedulingInfo != null) {
          return new QuerySchedulingInfo[]{querySchedulingInfo};
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
  }

  @Override
  public String getStatusHtml() {
    StringBuilder sb = new StringBuilder();

    Collection<QueryInProgress> runningQueries = queryJobManager.getRunningQueries();

    String runningQueryList = "";
    String prefix = "";

    if (runningQueries != null) {
      for (QueryInProgress eachQuery: runningQueries) {
        runningQueryList += prefix + eachQuery.getQueryId();
        prefix = "<br/>";
      }
    }

    String waitingQueryList = "";
    prefix = "";
    synchronized (pool) {
      if (runningQueries != null) {
        for (QuerySchedulingInfo eachQuery : pool) {
          waitingQueryList += prefix + eachQuery.getQueryId() +
              "<input id=\"btnSubmit\" type=\"submit\" value=\"Remove\" onClick=\"javascript:killQuery('" + eachQuery.getQueryId() + "');\">";
          prefix = "<br/>";
        }
      }
    }

    sb.append("<table border=\"1\" width=\"100%\" class=\"border_table\">");
    sb.append("<tr><th width='200'>Queue</th><th width='100'>Min Slot</th><th width='100'>Max Slot</th><th>Running Query</th><th>Waiting Queries</th></tr>");
    sb.append("<tr>");
    sb.append("<td>").append(DEFAULT_QUEUE_NAME).append("</td>");
    sb.append("<td>-</td>");
    sb.append("<td>-</td>");
    sb.append("<td>").append(runningQueryList).append("</td>");
    sb.append("<td>").append(waitingQueryList).append("</td>");
    sb.append("</tr>");
    sb.append("</table>");
    return sb.toString();
  }
}
