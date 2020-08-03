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

package org.apache.arrow.dataset.filter.scalar.timestamp;

import org.apache.arrow.dataset.filter.Expression;
import org.apache.arrow.dataset.filter.scalar.ScalarExpression;
import org.apache.arrow.dataset.filter.util.Util;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;

public class TimeStampMilliExpression extends TimeStampExpression {

    private final NullableTimeStampMilliHolder holder;

    public TimeStampMilliExpression(TimeStampMilliHolder holder) {
        this(holder.isSet, holder.value);
    }

    public TimeStampMilliExpression(NullableTimeStampMilliHolder holder) {
        this(holder.isSet, holder.value);
    }

    private TimeStampMilliExpression(int isSet, long value) {
        NullableTimeStampMilliHolder clone = new NullableTimeStampMilliHolder();
        clone.isSet = isSet;

        if (clone.isSet == Util.INT_VALUE_IF_IS_SET_TRUE) {
            clone.value = value;
        }

        this.holder = clone;
    }

    @Override
    public StructVector toVector(String vectorName, BufferAllocator allocator) {
        StructVector vector = new StructVector(vectorName, allocator, Expression.structVectorFieldType, null);
        
        Util.addTimeStampMilliVectorAsChild(vector, "c1", holder);
        Util.addIntVectorAsChild(vector, "c2", ScalarExpression.TYPE);

        vector.setValueCount(2);
        
        return vector;
    }

    @Override
    public Expression deepClone() {
        return new TimeStampMilliExpression(holder);
    }
}