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
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.master.querymaster.QueryJobManager;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractScheduler implements Scheduler {
  private static final Log LOG = LogFactory.getLog(AbstractScheduler.class.getName());

  protected Thread queryProcessor;
  protected AtomicBoolean stopped = new AtomicBoolean();
  protected QueryJobManager queryJobManager;

  protected abstract QuerySchedulingInfo[] getScheduledQueries();
  protected abstract void addQueryToQueue(QuerySchedulingInfo querySchedulingInfo) throws Exception;

  @Override
  public void init(QueryJobManager queryJobManager) {
    this.queryJobManager = queryJobManager;
    this.queryProcessor = new Thread(new QueryProcessor());
    this.queryProcessor.setName("Query Processor");
  }

  @Override
  public void start() {
    queryProcessor.start();
  }

  @Override
  public void stop() {
    if (stopped.getAndSet(true)) {
      return;
    }
    synchronized (queryProcessor) {
      queryProcessor.interrupt();
    }
  }

  @Override
  public void addQuery(QueryInProgress queryInProgress) throws Exception {
    QuerySchedulingInfo querySchedulingInfo =
        new QuerySchedulingInfo(queryInProgress.getQueryId(), 1, queryInProgress.getStartTime(), queryInProgress.getSession());
    addQueryToQueue(querySchedulingInfo);
    wakeupProcessor();
  }

  protected void wakeupProcessor() {
    synchronized (queryProcessor) {
      queryProcessor.notifyAll();
    }
  }

  private final class QueryProcessor implements Runnable {
    @Override
    public void run() {

      QuerySchedulingInfo[] queries;

      while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
        queries = getScheduledQueries();

        if (queries != null && queries.length > 0) {
          for (QuerySchedulingInfo eachQuery : queries) {
            try {
              queryJobManager.startQueryJob(eachQuery.getQueryId());
            } catch (Throwable t) {
              LOG.error("Exception during query startup:", t);
              queryJobManager.stopQuery(eachQuery.getQueryId());
            }
          }
        }

        synchronized (queryProcessor) {
          try {
            queryProcessor.wait(100);
          } catch (InterruptedException e) {
            if (stopped.get()) {
              break;
            }
            LOG.warn("Exception during shutdown: ", e);
          }
        }
      }
    }
  }
}
