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

package org.apache.arrow.dataset.jni;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;

/**
 * Native implementation of {@link Scanner}.
 */
public class NativeScanner implements Scanner {

  private final NativeContext context;
  private final long scannerId;

  public NativeScanner(NativeContext context, long scannerId) {
    this.context = context;
    this.scannerId = scannerId;
  }

  @Override
  public Iterable<? extends ScanTask> scan() {
    return Arrays.stream(JniWrapper.get().getScanTasksFromScanner(scannerId))
        .mapToObj(id -> new NativeScanTask(context, id))
        .collect(Collectors.toList());
  }

  @Override
  public void close() throws Exception {
    JniWrapper.get().closeScanner(scannerId);
  }
}
