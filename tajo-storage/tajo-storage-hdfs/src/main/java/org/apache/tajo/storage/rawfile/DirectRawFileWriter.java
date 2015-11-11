/*
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

package org.apache.tajo.storage.rawfile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.tuple.memory.OffHeapRowBlockUtils.TupleConverter;
import org.apache.tajo.tuple.memory.RowWriter;
import org.apache.tajo.tuple.memory.UnSafeTuple;
import org.apache.tajo.unit.StorageUnit;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class DirectRawFileWriter extends FileAppender {
  private static final Log LOG = LogFactory.getLog(DirectRawFileWriter.class);

  public static final String WRITE_BUFFER_SIZE = "tajo.storage.raw.io.write-buffer.bytes";
  public static final int DEFAULT_BUFFER_SIZE = 128 * StorageUnit.KB;
  public static final float BUFFER_THRESHHOLD = 0.9f;

  protected FileChannel channel;

  protected RandomAccessFile randomAccessFile;
  protected FSDataOutputStream fos;
  protected long pos;
  protected TableStatistics stats;

  protected TupleConverter tupleConverter;
  protected MemoryRowBlock rowBlock;
  protected boolean analyzeField;
  protected boolean hasExternalBuf;
  protected boolean isLocal;

  public DirectRawFileWriter(Configuration conf, TaskAttemptId taskAttemptId,
                             final Schema schema, final TableMeta meta, final Path path)
      throws IOException {
    this(conf, taskAttemptId, schema, meta, path, null);
  }

  public DirectRawFileWriter(Configuration conf, TaskAttemptId taskAttemptId,
                             final Schema schema, final TableMeta meta, final Path path,
                             MemoryRowBlock rowBlock) throws IOException {
    super(conf, taskAttemptId, schema, meta, path);
    this.rowBlock = rowBlock;
    this.hasExternalBuf = rowBlock != null;
  }

  private Schema columns;
  public void setStatColumns(Schema columns) {
     this.columns = columns;
  }

  @Override
  public void init() throws IOException {
    File file;
    FileSystem fs = path.getFileSystem(conf);

    if (fs instanceof LocalFileSystem) {
      try {
        if (path.toUri().getScheme() != null) {
          file = new File(path.toUri());
        } else {
          file = new File(path.toString());
        }
      } catch (IllegalArgumentException iae) {
        throw new IOException(iae);
      }

      randomAccessFile = new RandomAccessFile(file, "rw");
      channel = randomAccessFile.getChannel();
      isLocal = true;
    } else {
      fos = fs.create(path, true);
      isLocal = false;
    }

    if (enabledStats) {
      if (ShuffleType.RANGE_SHUFFLE == PlannerUtil.getShuffleType(
          meta.getOption(StorageConstants.SHUFFLE_TYPE,
              PlannerUtil.getShuffleType(ShuffleType.NONE_SHUFFLE)))) {
        this.analyzeField = true;
        this.stats = new TableStatistics(this.schema, this.columns);
      } else {
        this.stats = new TableStatistics(this.schema);
      }
    }

    if (rowBlock == null) {
      int bufferSize = conf.getInt(WRITE_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
      rowBlock = new MemoryRowBlock(SchemaUtil.toDataTypes(schema), bufferSize, true, meta.getDataFormat());
    }

    tupleConverter = initConverter();

    pos = 0;
    super.init();
  }

  public TupleConverter initConverter() {
    switch (meta.getDataFormat()) {
    case BuiltinStorages.DRAW:
      return getDrawConverter();
    case BuiltinStorages.RAW:
      return getRawConverter();
    default:
      throw new TajoInternalError(new UnsupportedException());
    }
  }

  private TupleConverter getDrawConverter() {
    return new TupleConverter() {

      @Override
      public void convert(Tuple tuple, RowWriter writer) {
        if (analyzeField) {
          if (tuple instanceof UnSafeTuple) {

            for (int i = 0; i < writer.dataTypes().length; i++) {
              // it is to calculate min/max values, and it is only used for the intermediate file.
              stats.analyzeField(i, tuple);
            }
            // write direct to memory
            writer.putTuple(tuple);
          } else {
            writer.startRow();

            for (int i = 0; i < writer.dataTypes().length; i++) {
              // it is to calculate min/max values, and it is only used for the intermediate file.
              stats.analyzeField(i, tuple);
              writeField(i, tuple, writer);
            }
            writer.endRow();
          }
        } else {
          // write direct to memory
          writer.putTuple(tuple);
        }
      }
    };
  }

  private TupleConverter getRawConverter() {
    return new TupleConverter() {

      @Override
      public void convert(Tuple tuple, RowWriter writer) {
        writer.startRow();

        for (int i = 0; i < writer.dataTypes().length; i++) {
          // it is to calculate min/max values, and it is only used for the intermediate file.
          if (analyzeField) {
            stats.analyzeField(i, tuple);
          }
          writeField(i, tuple, writer);
        }
        writer.endRow();
      }
    };
  }

  @Override
  public long getOffset() throws IOException {
    return hasExternalBuf ? pos : pos + rowBlock.getMemory().writerPosition();
  }

  public void writeRowBlock(MemoryRowBlock rowBlock) throws IOException {
    if(isLocal) {
      pos += rowBlock.getMemory().writeTo(channel);
    } else {
      pos += rowBlock.getMemory().writeTo(fos);
    }

    if (enabledStats) {
      stats.incrementRows(rowBlock.rows());
    }
  }

  @Override
  public void addTuple(Tuple t) throws IOException {

    tupleConverter.convert(t, rowBlock.getWriter());

    if(rowBlock.usage() > BUFFER_THRESHHOLD) {
      writeRowBlock(rowBlock);
      rowBlock.clear();
    }
  }

  @Override
  public void flush() throws IOException {
    if(!hasExternalBuf && rowBlock.getMemory().isReadable()) {
      writeRowBlock(rowBlock);
      rowBlock.clear();
    }
  }

  @Override
  public void close() throws IOException {
    flush();

    if (enabledStats) {
      stats.setNumBytes(getOffset());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("RawFileAppender written: " + getOffset() + " bytes, path: " + path);
    }

    IOUtils.cleanup(LOG, channel, randomAccessFile, fos);
    if(!hasExternalBuf && rowBlock != null) {
      rowBlock.release();
    }
  }

  @Override
  public TableStats getStats() {
    if (enabledStats) {
      stats.setNumBytes(pos);
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
