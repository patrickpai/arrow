/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.dataset.scanner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

/**
 * Options used during scanning.
 */
public class ScanOptions {
  private final String[] columns;
  private final long batchSize;
  private final byte[] filter;

  public ScanOptions(String[] columns, long batchSize) throws Exception {
    this(columns, batchSize, null);
  }

  /**
   * Constructor.
   * 
   * @param columns     Projected columns
   * @param batchSize   Maximum row number of each returned {@link org.apache.arrow.vector.VectorSchemaRoot}
   * @param filter      Filter in record batch representation
   * @throws Exception  On error
   */
  public ScanOptions(String[] columns, long batchSize, VectorSchemaRoot filter) throws Exception {
    this.columns = columns;
    this.batchSize = batchSize;
    this.filter = writeFilterToByteArray(filter);
  }

  public String[] getColumns() {
    return columns;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public byte[] getFilter() {
    return filter;
  }

  /**
   * Writes a filter (in record batch representation) to a byte array using IPC file format.
   * 
   * Returns byte array containing filter if the write was successful.
   * Otherwise, returns null;
   * 
   * @param root VectorSchemaRoot containing filter in record batch representation
   */
  private byte[] writeFilterToByteArray(VectorSchemaRoot root) throws Exception {
    if (root == null) {
      return null;
    }
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    try (ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))) {
      // Write filter (in record batch representation) to IPC file format
      writer.start();
      writer.writeBatch();
      writer.end();

      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to write filter to byte array.", e);
    }
  }
}
