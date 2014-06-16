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
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.querymaster.QueryJobManager;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * tajo-multiple-queue.xml
 *
 */
public class MultiQueueFiFoScheduler extends AbstractScheduler {
  private static final Log LOG = LogFactory.getLog(MultiQueueFiFoScheduler.class.getName());

  public static final String QUEUE_KEY_REPFIX = "tajo.multiple.queue";
  public static final String QUEUE_NAMES_KEY = QUEUE_KEY_REPFIX + ".names";

  private Map<String, QueueProperty> queueProperties = new HashMap<String, QueueProperty>();
  private Map<String, LinkedList<QuerySchedulingInfo>> queues = new HashMap<String, LinkedList<QuerySchedulingInfo>>();

  // QueryId -> Queue Name
  private Map<QueryId, String> queryAssignedMap = new HashMap<QueryId, String>();
  private Map<QueryId, QuerySchedulingInfo> runningQueries = new HashMap<QueryId, QuerySchedulingInfo>();

  private PropertyReloader propertyReloader;
  private Random rand;

  static class QueueProperty {
    String queueName;
    int minCapacity;
    int maxCapacity;
  }

  @Override
  public void init(QueryJobManager queryJobManager) {
    super.init(queryJobManager);

    rand = new Random(System.currentTimeMillis());
    initQueue();
  }

  private void reorganizeQueue(List<QueueProperty> newQueryList) {
    // TODO
    Set<String> previousQueueNames = queues.keySet();
  }

  private void initQueue() {
    List<QueueProperty> queueList = loadQueueProperty(queryJobManager.getMasterContext().getConf());

    if (!queues.isEmpty()) {
      reorganizeQueue(queueList);
      return;
    }

    for (QueueProperty eachQueue: queueList) {
      LinkedList<QuerySchedulingInfo> queue = new LinkedList<QuerySchedulingInfo>();
      queues.put(eachQueue.queueName, queue);
      queueProperties.put(eachQueue.queueName, eachQueue);
    }
  }

  public static List<QueueProperty> loadQueueProperty(TajoConf tajoConf) {
    TajoConf queueConf = new TajoConf(tajoConf);
    queueConf.addResource("tajo-multiple-queue.xml");

    List<QueueProperty> queueList = new ArrayList<QueueProperty>();

    String queueNameProperty = queueConf.get(QUEUE_NAMES_KEY);
    if (queueNameProperty == null || queueNameProperty.isEmpty()) {
      QueueProperty queueProperty = new QueueProperty();
      queueProperty.queueName = DEFAULT_QUEUE_NAME;
      queueProperty.maxCapacity = -1;   // unlimited

      queueList.add(queueProperty);

      return queueList;
    }

    String[] queueNames = queueNameProperty.split(",");
    for (String eachQueue: queueNames) {
      String capacityPropertyKey = QUEUE_KEY_REPFIX + "." + eachQueue + ".capacity";
      String capacity = queueConf.get(capacityPropertyKey);
      if (capacity == null || capacity.isEmpty()) {
        LOG.error("No " + capacityPropertyKey + " in tajo-multiple-queue.xml");
        continue;
      }

      QueueProperty queueProperty = new QueueProperty();
      queueProperty.queueName = eachQueue;
      queueProperty.maxCapacity = Integer.parseInt(capacity);
      queueList.add(queueProperty);
    }

    return queueList;
  }

  @Override
  public void start() {
    super.start();

//    propertyReloader = new PropertyReloader();
//    propertyReloader.start();
  }

  @Override
  public void stop() {
    super.stop();
    if (propertyReloader != null) {
      propertyReloader.interrupt();
    }
  }

  @Override
  public Mode getMode() {
    return Mode.FIFO_MULTI_QUEUE;
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public void notifyQueryStop(QueryId queryId) {
    synchronized (queues) {
      String assignedQueueName = queryAssignedMap.remove(queryId);
      if (assignedQueueName == null) {
        LOG.error("Can't get queue name from queryAssignedMap: " + queryId.toString());
        return;
      }

      LinkedList<QuerySchedulingInfo> queue = queues.get(assignedQueueName);
      if (queue == null) {
        LOG.error("Can't get queue from multiple queue: " + queryId.toString() + ", queue=" + assignedQueueName);
        return;
      }

      QuerySchedulingInfo runningQuery = runningQueries.remove(queryId);
      if (runningQuery == null) {
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
          LOG.error("No query info in the queue: " + queryId + ", queue=" + assignedQueueName);
          return;
        }
      }

      // It this queue is empty, find a query which can be assigned this queue.
      if (queue.isEmpty()) {
        String reorganizeTargetQueueName = null;
        QuerySchedulingInfo reorganizeTargetQuery = null;
        int maxQueueSize = Integer.MIN_VALUE;
        for (Map.Entry<String, LinkedList<QuerySchedulingInfo>> entry: queues.entrySet()) {
          String eachQueueName = entry.getKey();
          LinkedList<QuerySchedulingInfo> eachQueue = entry.getValue();
          for (QuerySchedulingInfo eachQuery: eachQueue) {
            if (eachQuery.getCandidateQueueNames().contains(assignedQueueName)) {
              if (eachQueue.size() > maxQueueSize) {
                reorganizeTargetQuery = eachQuery;
                reorganizeTargetQueueName = eachQueueName;
                maxQueueSize = eachQueue.size();
                break;
              }
            }
          }
        }

        if (reorganizeTargetQueueName != null) {
          LinkedList<QuerySchedulingInfo> reorganizeTargetQueue = queues.get(reorganizeTargetQueueName);
          reorganizeTargetQueue.remove(reorganizeTargetQuery);

          reorganizeTargetQuery.setAssignedQueueName(assignedQueueName);
          queue.add(reorganizeTargetQuery);
        }
      }
    }
    wakeupProcessor();
  }

  @Override
  protected QuerySchedulingInfo[] getScheduledQueries() {
    Set<String> readyQueueNames = new HashSet<String>(queues.keySet());

    for (QuerySchedulingInfo eachQuery : runningQueries.values()) {
      readyQueueNames.remove(eachQuery.getAssignedQueueName());
    }
    if (readyQueueNames.isEmpty()) {
      return null;
    }

    List<QuerySchedulingInfo> queries = new ArrayList<QuerySchedulingInfo>();

    for (String eachQueueName: readyQueueNames) {
      LinkedList<QuerySchedulingInfo> queue = queues.get(eachQueueName);

      if (queue == null) {
        LOG.warn("No queue for " + eachQueueName);
        continue;
      }

      if (!queue.isEmpty()) {
        queries.add(queue.poll());
      }
    }

    return queries.toArray(new QuerySchedulingInfo[]{});
  }

  @Override
  protected boolean addQueryToQueue(QuerySchedulingInfo querySchedulingInfo) {
    String submitQueueNameProperty = querySchedulingInfo.getSession().getVariable(ConfVars.JOB_QUEUE_NAMES.varname,
        ConfVars.JOB_QUEUE_NAMES.defaultVal);

    String[] queueNames = submitQueueNameProperty.split(",");
    Set<String> candidateQueueNames = TUtil.newHashSet(queueNames);

    String selectedQueue = null;
    int minQueueSize = Integer.MIN_VALUE;

    synchronized (queues) {
      for (String eachQueue: queues.keySet()) {
        LinkedList<QuerySchedulingInfo> queue = queues.get(eachQueue);
        if (queue != null && queue.size() < minQueueSize) {
          selectedQueue = eachQueue;
          minQueueSize = queue.size();
        }
      }

      if (selectedQueue != null) {
        LinkedList<QuerySchedulingInfo> queue = queues.get(selectedQueue);
        querySchedulingInfo.setAssignedQueueName(selectedQueue);
        querySchedulingInfo.setCandidateQueueNames(candidateQueueNames);
        queue.push(querySchedulingInfo);

        queryAssignedMap.put(querySchedulingInfo.getQueryId(), selectedQueue);
      }
    }

    return selectedQueue != null;
  }

  @Override
  public String getStatusHtml() {
    StringBuilder sb = new StringBuilder();

    String runningQueryList = "";
    String waitingQueryList = "";

    String prefix = "";

    sb.append("<table>");
    sb.append("<tr><th>Queue</th><th>Min Slot</th><th>Max Slot</th><th>Running Query<th><th>Waiting Queries</th></tr>");

    synchronized (queues) {
      for (String eachQueueName : queues.keySet()) {
        QueueProperty queryProperty = queueProperties.get(eachQueueName);
        sb.append("<tr><td>").append(eachQueueName).append("</td>");

        sb.append("<tr><td>").append(queryProperty.minCapacity).append("</td>");
        sb.append("<tr><td>").append(queryProperty.maxCapacity).append("</td>");

        QuerySchedulingInfo runningQueryInfo = null;
        for (QuerySchedulingInfo eachQuery : runningQueries.values()) {
          if (eachQueueName.equals(eachQuery.getAssignedQueueName())) {
            runningQueryInfo = eachQuery;
            break;
          }
        }
        if (runningQueryInfo != null) {
          runningQueryList += prefix + runningQueryInfo.getQueryId();
        }

        prefix = "";
        for (QuerySchedulingInfo eachQuery : queues.get(eachQueueName)) {
          waitingQueryList += prefix + eachQuery.getQueryId() +
              "<input id=\"btnSubmit\" type=\"submit\" value=\"Remove\" onClick=\"javascript:killQuery('" + eachQuery.getQueryId() + "');\">";
          prefix = "<br/>";
        }

        sb.append("<tr><td>").append(runningQueryList).append("</td>");
        sb.append("<tr><td>").append(waitingQueryList).append("</td>");
      }
    }

    sb.append("</table>");
    return sb.toString();
  }

  private class PropertyReloader extends Thread {
    public PropertyReloader() {
      super("MultiQueueFiFoScheduler-PropertyReloader");
    }

    @Override
    public void run() {
      LOG.info("MultiQueueFiFoScheduler-PropertyReloader started");
      while (!stopped.get()) {
        try {
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }
}
