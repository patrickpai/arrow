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

package org.apache.arrow.dataset.filter.scalar;

import org.apache.arrow.dataset.filter.Expression;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.complex.StructVector;

public class NullScalarExpression extends ScalarExpression {

    private final Expression expr;

    public NullScalarExpression(ScalarExpression expr) {
        this.expr = expr.deepClone();
    }

    @Override
    public StructVector toVector(String vectorName, BufferAllocator allocator) {
        return expr.toVector("c1", allocator);
    }

    @Override
    public Expression deepClone() {
        return new NullScalarExpression((ScalarExpression) expr);
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(expr);
    }
}