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

package org.apache.tajo.storage.text;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.apache.tajo.storage.BufferPool;
import org.apache.tajo.storage.ByteBufInputChannel;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ByteBufLineReader implements Closeable {
  private static int DEFAULT_BUFFER = 64 * 1024;

  private int bufferSize;
  private long readBytes;
  private final ByteBuf buffer;
  private final ByteBufInputChannel channel;
  private final AtomicInteger tempReadBytes = new AtomicInteger();
  private final LineSplitProcessor processor = new LineSplitProcessor();

  public ByteBufLineReader(ByteBufInputChannel channel) {
    this(channel, BufferPool.getAllocator().directBuffer(DEFAULT_BUFFER));
  }

  public ByteBufLineReader(ByteBufInputChannel channel, ByteBuf buf) {
    this.readBytes = 0;
    this.channel = channel;
    this.buffer = buf;
    this.bufferSize = buf.capacity();
  }

  public long readBytes() {
    return readBytes - buffer.readableBytes();
  }

  public long available() throws IOException {
    return channel.available() + buffer.readableBytes();
  }

  @Override
  public void close() throws IOException {
    this.buffer.release();
    this.channel.close();
  }

  public String readLine() throws IOException {
    ByteBuf buf = readLineBuf(tempReadBytes);
    if (buf != null) {
      return buf.toString(CharsetUtil.UTF_8);
    }
    return null;
  }

  private void compactBuf(ByteBuf buf) {
    int remainBytes = buf.readableBytes();
    ByteBuf tailBuf = buf.slice(buf.readerIndex(), remainBytes);
    buf.resetReaderIndex();
    buf.resetWriterIndex();
    buf.writeBytes(tailBuf);
  }

  private ByteBuf readNextBuffer() throws IOException {

    int remainBytes = 0;
    if (this.readBytes > 0) {
      compactBuf(this.buffer);
      if (!this.buffer.isWritable()) {
        // a line bytes is large than the buffer
        this.buffer.ensureWritable(bufferSize);
      }
    }

    boolean release = true;
    try {
      int readBytes = 0;
      for (; ; ) {
        int localReadBytes = buffer.writeBytes(channel, bufferSize - readBytes);
        if (localReadBytes < 0) {
          break;
        }
        readBytes += localReadBytes;
        if (readBytes == bufferSize) {
          break;
        }
      }
      this.readBytes += readBytes;
      release = false;
      this.buffer.readerIndex(this.buffer.readerIndex() + remainBytes);
      return buffer;
    } finally {
      if (release) {
        buffer.release();
      }
    }
  }

  /**
   * Read a line terminated by one of CR, LF, or CRLF.
   */
  public ByteBuf readLineBuf(AtomicInteger reads) throws IOException {
    int startIndex = buffer.readerIndex();
    int readBytes;
    int readable;
    int newlineLength; //length of terminating newline

    loop:
    while (true) {
      readable = buffer.readableBytes();
      if (readable <= 0) {
        buffer.readerIndex(startIndex);
        ByteBuf buf = readNextBuffer(); //fill buffer
        if (!buf.isReadable()) {
          return null;
        } else {
          startIndex = 0; // reset the buffer position
        }
        readable = buffer.readableBytes();
      }

      int endIndex = buffer.forEachByte(startIndex, readable, processor);
      if (endIndex < 0) {
        buffer.readerIndex(buffer.writerIndex());
      } else {
        buffer.readerIndex(endIndex + 1);
        readBytes = buffer.readerIndex() - startIndex;
        if (processor.isPrevCharCR() && buffer.isReadable()
            && buffer.getByte(buffer.readerIndex()) == LineSplitProcessor.LF) {
          buffer.skipBytes(1);
          newlineLength = 2;
        } else {
          newlineLength = 1;
        }
        break loop;
      }
    }
    reads.set(readBytes);
    return buffer.slice(startIndex, readBytes - newlineLength);
  }
}
