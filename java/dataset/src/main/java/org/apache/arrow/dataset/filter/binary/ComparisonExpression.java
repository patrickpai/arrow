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

package org.apache.arrow.dataset.filter.binary;

import org.apache.arrow.dataset.filter.Expression;
import org.apache.arrow.dataset.filter.util.Util;
import org.apache.arrow.flatbuf.ExpressionType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.StructVector;

public class ComparisonExpression extends BinaryExpression {

    private static final byte TYPE = ExpressionType.COMPARISON;
    private final byte op;

    public ComparisonExpression(Expression leftOperand, byte op, Expression rightOperand) {
        super(leftOperand.deepClone(), rightOperand.deepClone());
        this.op = op;
    }

    @Override
    public StructVector toVector(String vectorName, BufferAllocator allocator) {
        StructVector vector = new StructVector(vectorName, allocator, Expression.structVectorFieldType, null);

        StructVector left = getLeftOperand().toVector("c1", allocator);
        StructVector right = getRightOperand().toVector("c2", allocator);

        StructVector leftInVector = vector.addOrGetStruct("c1");
        Util.transfer(left, leftInVector);
        StructVector rightInVector = vector.addOrGetStruct("c2");
        Util.transfer(right, rightInVector);

        Util.addIntVectorAsChild(vector, "c3", op);
        Util.addIntVectorAsChild(vector, "c4", ComparisonExpression.TYPE);

        vector.setValueCount(4);

        left.close();
        right.close();
        
        return vector;
    }

    @Override
    public Expression deepClone() {
        return new ComparisonExpression(super.getLeftOperand(), this.op, super.getRightOperand());
    }
}