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

package org.apache.arrow.dataset.filter.unary;

import org.apache.arrow.dataset.filter.Expression;
import org.apache.arrow.dataset.filter.util.FieldTypes;
import org.apache.arrow.dataset.filter.util.Util;
import org.apache.arrow.flatbuf.ExpressionType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

public class InExpression<T extends ValueVector> extends UnaryExpression {

    private static final byte TYPE = ExpressionType.IN;
    private final BufferAllocator allocator;
    private final T set; // The set against which the operand will be compared

    public InExpression(BufferAllocator allocator, Expression operand, T set) {
        super(operand.deepClone());
        this.allocator = allocator;
        this.set = deepCloneSet(set);
    }

    @Override
    public StructVector toVector(String vectorName, BufferAllocator allocator) {
        StructVector vector = new StructVector(vectorName, allocator, Expression.structVectorFieldType, null);

        StructVector operand = getOperand().toVector("c1", allocator);

        // Child 1
        StructVector operandInVector = vector.addOrGetStruct("c1");
        Util.transfer(operand, operandInVector);

        // Child 2
        ListVector list = vector.addOrGetList("c2");
        list.startNewValue(0);

        FieldType fieldType = FieldType.nullable(set.getMinorType().getType());
        AddOrGetResult<T> result = list.addOrGetVector(fieldType);
        if (!result.isCreated()) {
            throw new RuntimeException("Failed to add or get vector.");
        }

        T setInVector = result.getVector();
        T setClone = deepCloneSet(set);
        setClone.makeTransferPair(setInVector).transfer(); // TODO: may have to clone set first, if we call toVector > 1 time
        list.endValue(0, setInVector.getValueCount());
        list.setValueCount(1);

        // Child 3
        Util.addIntVectorAsChild(vector, "c3", InExpression.TYPE);

        vector.setValueCount(3);

        operand.close();
        setClone.close();
        
        return vector;
    }

    @Override
    public void close() throws Exception {
        super.close();
        AutoCloseables.close(set);
    }
    
    @Override
    public Expression deepClone() {
        return new InExpression<T>(allocator, super.getOperand(), set);
    }

    private T deepCloneSet(T set) {
        T clone = (T) set.getField().createVector(allocator);

        for (int idx = 0; idx < set.getValueCount(); idx++) {
            clone.copyFromSafe(idx, idx, set);
        }
        
        clone.setValueCount(set.getValueCount());
        return clone;
    }
}