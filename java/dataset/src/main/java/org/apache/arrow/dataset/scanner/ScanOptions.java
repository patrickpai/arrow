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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;

/**
 * Options used during scanning.
 */
public class ScanOptions {
  private final String[] columns;
  private final long batchSize;
  private ArrowBuf filterBuffer;
  private long filterSize = -1; // size in bytes

  /**
   * Constructor.
   * @param columns Projected columns
   * @param batchSize Maximum row number of each returned {@link org.apache.arrow.vector.VectorSchemaRoot}
   */
  public ScanOptions(String[] columns, long batchSize) {
    this.columns = columns;
    this.batchSize = batchSize;
  }

  public ScanOptions(String[] columns, long batchSize, String filterAsHexString) {
    this.columns = columns;
    this.batchSize = batchSize;
    this.filterBuffer = writeFilterToDirectBuffer(filterAsHexString);
  }

  public String[] getColumns() {
    return columns;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public long getFilterMemoryAddress() {
    return filterBuffer != null ? filterBuffer.memoryAddress() : -1; // TODO: fix
//    return filterBuffer.memoryAddress();
  }

  public long getFilterSize() {
    return filterSize;
  }

  private ArrowBuf writeFilterToDirectBuffer(String filterAsHexString) {
    RootAllocator allocator = new RootAllocator(4096); // TODO: adjust limit
    ArrowBuf buffer = allocator.buffer(filterAsHexString.length() / 2); // TODO: adjust based on binary or hex string

    // write filter to buffer here
    for (int i = 0, j = 0; i < filterAsHexString.length(); i += 2, j++) {
      String byteInHex = filterAsHexString.substring(i, i + 2);
      int num = Integer.parseUnsignedInt(byteInHex, 16);
      buffer.setByte(j, num);
    }

    this.filterSize = filterAsHexString.length() / 2; // TODO: adjust

    return buffer;
  }
}
