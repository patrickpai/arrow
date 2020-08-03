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

package org.apache.arrow.dataset.filter.scalar.integers;

import org.apache.arrow.dataset.filter.Expression;
import org.apache.arrow.dataset.filter.scalar.ScalarExpression;
import org.apache.arrow.dataset.filter.util.Util;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.SmallIntHolder;

public class Int16ScalarExpression extends ScalarExpression {

    private NullableSmallIntHolder holder;

    public Int16ScalarExpression(SmallIntHolder holder) {
        NullableSmallIntHolder clone = new NullableSmallIntHolder();
        clone.isSet = holder.isSet;
        clone.value = holder.value;

        this.holder = clone;
    }

    public Int16ScalarExpression(NullableSmallIntHolder holder) {
        NullableSmallIntHolder clone = new NullableSmallIntHolder();
        clone.isSet = holder.isSet;
        if (clone.isSet == Util.INT_VALUE_IF_IS_SET_TRUE) {
            clone.value = holder.value;
        }

        this.holder = clone;
    }

    @Override
    public StructVector toVector(String vectorName, BufferAllocator allocator) {
        StructVector vector = new StructVector(vectorName, allocator, Expression.structVectorFieldType, null);
        
        Util.addSmallIntVectorAsChild(vector, "c1", holder);
        Util.addIntVectorAsChild(vector, "c2", ScalarExpression.TYPE);

        vector.setValueCount(2);

        return vector;
    }

    @Override
    public Expression deepClone() {
        return new Int16ScalarExpression(holder);
    }

    @Override
    public void close() throws Exception {
        // noop
    }
}