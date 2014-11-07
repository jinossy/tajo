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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.querymaster.QueryJobManager;

import java.util.*;

/**
 * tajo-multiple-queue.xml
 *
 */
public class FairScheduler extends AbstractScheduler {
  private static final Log LOG = LogFactory.getLog(FairScheduler.class.getName());

  public static final String QUEUE_KEY_REPFIX = "tajo.fair.queue";
  public static final String QUEUE_NAMES_KEY = QUEUE_KEY_REPFIX + ".names";

  private Map<String, QueueProperty> queueProperties = new HashMap<String, QueueProperty>();
  private Map<String, LinkedList<QuerySchedulingInfo>> queues = new HashMap<String, LinkedList<QuerySchedulingInfo>>();
  private Map<QueryId, QuerySchedulingInfo> runningQueries = Maps.newConcurrentMap();
  private Map<QueryId, String> queryAssignedMap = Maps.newConcurrentMap();

  @Override
  public void init(QueryJobManager queryJobManager) {
    super.init(queryJobManager);
    initQueue();
  }

  private void reorganizeQueue(List<QueueProperty> newQueryList) {
    Set<String> previousQueueNames = new HashSet<String>(queues.keySet());

    for (QueueProperty eachQueue: newQueryList) {
      queueProperties.put(eachQueue.getQueueName(), eachQueue);
      if (!previousQueueNames.remove(eachQueue.getQueueName())) {
        // not existed queue
        LinkedList<QuerySchedulingInfo> queue = new LinkedList<QuerySchedulingInfo>();
        queues.put(eachQueue.getQueueName(), queue);
        LOG.info("Queue [" + eachQueue + "] added");
      }
    }

    // Removed queue
    for (String eachRemovedQueue: previousQueueNames) {
      queueProperties.remove(eachRemovedQueue);

      LinkedList<QuerySchedulingInfo> queue = queues.remove(eachRemovedQueue);
      LOG.info("Queue [" + eachRemovedQueue + "] removed");
      if (queue != null) {
        for (QuerySchedulingInfo eachQuery: queue) {
          LOG.warn("Remove waiting query: " + eachQuery + " from " + eachRemovedQueue + " queue");
        }
      }

      queue.clear();
    }
  }

  private void initQueue() {
    List<QueueProperty> queueList = loadQueueProperty(queryJobManager.getMasterContext().getConf());

    synchronized (queues) {
      if (!queues.isEmpty()) {
        reorganizeQueue(queueList);
        return;
      }

      for (QueueProperty eachQueue : queueList) {
        LinkedList<QuerySchedulingInfo> queue = new LinkedList<QuerySchedulingInfo>();
        queues.put(eachQueue.getQueueName(), queue);
        queueProperties.put(eachQueue.getQueueName(), eachQueue);
        LOG.info("Queue [" + eachQueue + "] added");
      }
    }
  }

  public static List<QueueProperty> loadQueueProperty(TajoConf tajoConf) {
    TajoConf queueConf = new TajoConf(tajoConf);
    queueConf.addResource("tajo-fair-queue.xml");

    List<QueueProperty> queueList = new ArrayList<QueueProperty>();

    String queueNameProperty = queueConf.get(QUEUE_NAMES_KEY);
    if (queueNameProperty == null || queueNameProperty.isEmpty()) {
      LOG.warn("Can't found queue. added " + DEFAULT_QUEUE_NAME + " queue");
      QueueProperty queueProperty = new QueueProperty(DEFAULT_QUEUE_NAME, 1, -1);
      queueList.add(queueProperty);

      return queueList;
    }

    String[] queueNames = queueNameProperty.split(",");
    for (String eachQueue: queueNames) {
      String maxPropertyKey = QUEUE_KEY_REPFIX + "." + eachQueue + ".max";
      String minPropertyKey = QUEUE_KEY_REPFIX + "." + eachQueue + ".min";

      if (StringUtils.isEmpty(queueConf.get(maxPropertyKey)) || StringUtils.isEmpty(queueConf.get(minPropertyKey))) {
        LOG.error("Can't find " + maxPropertyKey + " or "+ minPropertyKey + " in tajo-fair-queue.xml");
        continue;
      }

      QueueProperty queueProperty = new QueueProperty(eachQueue,
          queueConf.getInt(minPropertyKey, 1),
          queueConf.getInt(maxPropertyKey, 1));
      queueList.add(queueProperty);
    }

    return queueList;
  }

  @Override
  public void start() {
    super.start();

  }

  @Override
  public void stop() {
    super.stop();
  }

  @Override
  public Mode getMode() {
    return Mode.FAIR;
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public int getRunningQueries(String queueName) {
    int runningSize = 0;
    for (QuerySchedulingInfo eachQuery : runningQueries.values()) {
      if(eachQuery.getAssignedQueueName().equals(queueName)){
        runningSize++;
      }
    }
    return runningSize;
  }

  @Override
  public void notifyQueryStop(QueryId queryId) {
    synchronized (queues) {
      QuerySchedulingInfo runningQuery = runningQueries.remove(queryId);
      String queueName = queryAssignedMap.remove(queryId);


      LinkedList<QuerySchedulingInfo> queue = queues.get(queueName);
      if (queue == null) {
        LOG.error("Can't get queue from multiple queue: " + queryId.toString() + ", queue=" + queueName);
        return;
      }

      // If the query is a waiting query, remove from a queue.
      LOG.info(queryId.toString() + " is not a running query. Removing from queue.");
      QuerySchedulingInfo stoppedQuery = null;
      for (QuerySchedulingInfo eachQuery: queue) {
        if (eachQuery.getQueryId().equals(queryId)) {
          stoppedQuery = eachQuery;
          break;
        }
      }

      if (stoppedQuery != null) {
        queue.remove(stoppedQuery);
      } else {
        LOG.error("No query info in the queue: " + queryId + ", queue=" + queueName);
        return;
      }
    }
    wakeupProcessor();
  }

  @Override
  protected QuerySchedulingInfo[] getScheduledQueries() {
    synchronized (queues) {
      List<QuerySchedulingInfo> queries = new ArrayList<QuerySchedulingInfo>();

      for (String eachQueueName : queues.keySet()) {
        int runningSize = getRunningQueries(eachQueueName);
        QueueProperty property = queueProperties.get(eachQueueName);

        if(property.getMaxCapacity() == -1){
          LinkedList<QuerySchedulingInfo> queue = queues.get(eachQueueName);
          if (queue != null && queue.size() > 0) {
            QuerySchedulingInfo querySchedulingInfo = queue.pollFirst();
            queries.add(querySchedulingInfo);
            runningQueries.put(querySchedulingInfo.getQueryId(), querySchedulingInfo);
          }
        } else {
          if (property.getMinCapacity() * (runningSize + 1) < property.getMaxCapacity()) {
            LinkedList<QuerySchedulingInfo> queue = queues.get(eachQueueName);
            if (queue != null && queue.size() > 0) {
              QuerySchedulingInfo querySchedulingInfo = queue.pollFirst();
              queries.add(querySchedulingInfo);
              runningQueries.put(querySchedulingInfo.getQueryId(), querySchedulingInfo);
            } else {
              continue;
            }
          }
        }
      }

      return queries.toArray(new QuerySchedulingInfo[]{});
    }
  }

  @Override
  protected void addQueryToQueue(QuerySchedulingInfo querySchedulingInfo) throws Exception {
    String submitQueueNameProperty = querySchedulingInfo.getQueryContext().get(ConfVars.JOB_QUEUE_NAMES.varname,
        ConfVars.JOB_QUEUE_NAMES.defaultVal);

    String queueName = submitQueueNameProperty.split(",")[0];
    synchronized (queues) {
      LinkedList<QuerySchedulingInfo> queue = queues.get(queueName);

      if (queue != null) {

        querySchedulingInfo.setAssignedQueueName(queueName);
        querySchedulingInfo.setCandidateQueueNames(Sets.newHashSet(queueName));
        queue.push(querySchedulingInfo);

        queryAssignedMap.put(querySchedulingInfo.getQueryId(), queueName);

        LOG.info(querySchedulingInfo.getQueryId() + " is assigned to the [" + queueName + "] queue");
      } else {
        throw new Exception("Can't find proper query queue(requested queue=" + submitQueueNameProperty + ")");
      }
    }
  }

  @Override
  public String getStatusHtml() {
    StringBuilder sb = new StringBuilder();

    String runningQueryList = "";
    String waitingQueryList = "";

    String prefix = "";

    sb.append("<table border=\"1\" width=\"100%\" class=\"border_table\">");
    sb.append("<tr><th width='200'>Queue</th><th width='100'>Min Slot</th><th width='100'>Max Slot</th><th>Running Query</th><th>Waiting Queries</th></tr>");

    synchronized (queues) {
      SortedSet<String> queueNames = new TreeSet<String>(queues.keySet());
      for (String eachQueueName : queueNames) {
        waitingQueryList = "";
        runningQueryList = "";

        QueueProperty queryProperty = queueProperties.get(eachQueueName);
        sb.append("<tr>");
        sb.append("<td>").append(eachQueueName).append("</td>");

        sb.append("<td align='right'>").append(queryProperty.getMinCapacity()).append("</td>");
        sb.append("<td align='right'>").append(queryProperty.getMaxCapacity()).append("</td>");

        for (QuerySchedulingInfo eachQuery : runningQueries.values()) {
          if (eachQueueName.equals(eachQuery.getAssignedQueueName())) {
            runningQueryList += prefix + eachQuery.getQueryId() + ",";
          }
        }

        prefix = "";
        for (QuerySchedulingInfo eachQuery : queues.get(eachQueueName)) {
          waitingQueryList += prefix + eachQuery.getQueryId() +
              "<input id=\"btnSubmit\" type=\"submit\" value=\"Remove\" onClick=\"javascript:killQuery('" + eachQuery.getQueryId() + "');\">";
          prefix = "<br/>";
        }

        sb.append("<td>").append(runningQueryList).append("</td>");
        sb.append("<td>").append(waitingQueryList).append("</td>");
        sb.append("</tr>");
      }
    }

    sb.append("</table>");
    return sb.toString();
  }

  private class PropertyReloader extends Thread {
    public PropertyReloader() {
      super("FairScheduler-PropertyReloader");
    }

    @Override
    public void run() {
      LOG.info("FairScheduler-PropertyReloader started");
      while (!stopped.get()) {
        try {
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
          break;
        }
        initQueue();
      }
    }
  }
}
