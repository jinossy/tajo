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

package org.apache.tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.plan.logical.ShuffleFileWriteNode;
import org.apache.tajo.storage.HashShuffleAppender;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.tuple.memory.RowBlock;
import org.apache.tajo.tuple.memory.RowWriter;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * <code>HashShuffleFileWriteExec</code> is a physical executor to store intermediate data into a number of
 * file outputs associated with shuffle keys. The file outputs are stored on local disks.
 */
public final class HashShuffleFileWriteExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(HashShuffleFileWriteExec.class);
  private ShuffleFileWriteNode plan;
  private final TableMeta meta;
  private Partitioner partitioner;
  private Map<Integer, HashShuffleAppender> appenderMap = new HashMap<>();
  private final int numShuffleOutputs;
  private final int [] shuffleKeyIds;
  private HashShuffleAppenderManager hashShuffleAppenderManager;
  private int maxBufferSize;
  private int initialRowBufferSize;
  private final DataType[] dataTypes;

  public HashShuffleFileWriteExec(TaskAttemptContext context,
                                  final ShuffleFileWriteNode plan, final PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    Preconditions.checkArgument(plan.hasShuffleKeys());
    this.plan = plan;
    if (plan.hasOptions()) {
      this.meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      this.meta = CatalogUtil.newTableMeta(plan.getStorageType());
    }
    // about the shuffle
    this.numShuffleOutputs = this.plan.getNumOutputs();
    int i = 0;
    this.shuffleKeyIds = new int [this.plan.getShuffleKeys().length];
    for (Column key : this.plan.getShuffleKeys()) {
      shuffleKeyIds[i] = inSchema.getColumnId(key.getQualifiedName());
      i++;
    }
    this.partitioner = new HashPartitioner(shuffleKeyIds, numShuffleOutputs);
    this.hashShuffleAppenderManager = context.getHashShuffleAppenderManager();
    this.maxBufferSize = context.getConf().getIntVar(ConfVars.SHUFFLE_HASH_APPENDER_BUFFER_SIZE);
    this.initialRowBufferSize = 128 * StorageUnit.KB;
    this.dataTypes = SchemaUtil.toDataTypes(outSchema);
  }

  @Override
  public void init() throws IOException {
    super.init();
  }
  
  private HashShuffleAppender getAppender(int partId) throws IOException {
    HashShuffleAppender appender = appenderMap.get(partId);
    if (appender == null) {
      appender = hashShuffleAppenderManager.getAppender(context.getConf(),
          context.getTaskId().getTaskId().getExecutionBlockId(), partId, meta, outSchema);
      appenderMap.put(partId, appender);
    }
    return appender;
  }

  Map<Integer, RowBlock> partitionTuples = new HashMap<Integer, RowBlock>();
  long writtenBytes = 0L;
  long usedMem = 0;

  @Override
  public Tuple next() throws IOException {
    try {
      Tuple tuple;
      int partId;
      long numRows = 0;
      while (!context.isStopped() && (tuple = child.next()) != null) {
        numRows++;

        partId = partitioner.getPartition(tuple);
        RowBlock rowBlock = partitionTuples.get(partId);
        if (rowBlock == null) {
          rowBlock = new MemoryRowBlock(dataTypes, initialRowBufferSize);
          partitionTuples.put(partId, rowBlock);
        }

        RowWriter writer = rowBlock.getWriter();
        long prevUsedMem = rowBlock.getMemory().readableBytes();
        writer.addTuple(tuple);
        usedMem += (rowBlock.getMemory().readableBytes() - prevUsedMem);

        if (usedMem >= maxBufferSize) {
          List<Future<Integer>> resultList = Lists.newArrayList();
          for (Map.Entry<Integer, RowBlock> entry : partitionTuples.entrySet()) {
            int appendPartId = entry.getKey();
            HashShuffleAppender appender = getAppender(appendPartId);

            //flush buffers
            resultList.add(hashShuffleAppenderManager.
                writePartitions(appender, context.getTaskId(), entry.getValue(), false));

          }

          for (Future<Integer> future : resultList) {
            writtenBytes += future.get();
          }
          usedMem = 0;
        }
      }

      // write in-memory tuples
      List<Future<Integer>> resultList = Lists.newArrayList();
      for (Map.Entry<Integer, RowBlock> entry : partitionTuples.entrySet()) {
        int appendPartId = entry.getKey();
        HashShuffleAppender appender = getAppender(appendPartId);

        //flush buffers and release
        resultList.add(hashShuffleAppenderManager.
            writePartitions(appender, context.getTaskId(), entry.getValue(), true));

      }

      for (Future<Integer> future : resultList) {
        writtenBytes += future.get();
      }

      usedMem = 0;
      TableStats aggregated = (TableStats)child.getInputStats().clone();
      aggregated.setNumBytes(writtenBytes);
      aggregated.setNumRows(numRows);
      context.setResultStats(aggregated);

      partitionTuples.clear();

      return null;
    } catch (RuntimeException e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  @Override
  public void rescan() throws IOException {
    if(partitionTuples != null && partitionTuples.size() > 0){
      for (RowBlock rowBlock : partitionTuples.values()) {
        rowBlock.release();
      }
      partitionTuples.clear();
    }
  }

  @Override
  public void close() throws IOException{
    super.close();
    if (appenderMap != null) {
      appenderMap.clear();
      appenderMap = null;
    }

    if(partitionTuples != null && partitionTuples.size() > 0){
      for (RowBlock rowBlock : partitionTuples.values()) {
        rowBlock.release();
      }
      partitionTuples.clear();
    }

    partitioner = null;
    plan = null;

    progress = 1.0f;
  }
}