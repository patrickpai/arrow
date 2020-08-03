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

package org.apache.arrow.dataset.filter.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.TransferPair;

// TODO: reorder these later
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.TimeUnit;

public final class Util {

    public static final int INT_VALUE_IF_IS_SET_TRUE = 1;

    /**
     * Transfer underlying buffers of src StructVector to dest StructVector.
     * 
     * @param src
     * @param dest
     */
    public static void transfer(StructVector src, StructVector dest) {
        TransferPair tp = src.makeTransferPair(dest);
        tp.transfer();
    }
    
    public static VectorSchemaRoot loadIntoVectorSchemaRoot(StructVector vector) {
        List<FieldVector> allVectors = new ArrayList<>();

        for (int index = 0; index < vector.size(); index++) {
            allVectors.add(vector.getChildrenFromFields().get(index));
        }
        
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(allVectors);
        vectorSchemaRoot.setRowCount(1);
        return vectorSchemaRoot;
    }

    public static void addIntVectorAsChild(StructVector parent, String name, int value) {
        IntVector child = parent.addOrGet(name, FieldTypes.INT32, IntVector.class);
        child.allocateNew();
        child.setSafe(0, value);
        child.setIndexDefined(0);
        child.setValueCount(1);
        return;
    }

    public static void addVarCharVectorAsChild(StructVector parent, String name, String value) {
        VarCharVector child = parent.addOrGet(name, FieldTypes.STRING, VarCharVector.class);
        child.allocateNew();
        child.setSafe(0, value.getBytes());
        child.setIndexDefined(0);
        child.setValueCount(1);
        return;
    }

    /*----------------------------------------------------------------*
     |                                                                |
     |          Integer types                                         |
     |                                                                |
     *----------------------------------------------------------------*/

    public static void addUInt1VectorAsChild(StructVector parent, String name, NullableUInt1Holder holder) {
        UInt1Vector child = parent.addOrGet(name, FieldTypes.UINT8, UInt1Vector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addTinyIntVectorAsChild(StructVector parent, String name, NullableTinyIntHolder holder) {
        TinyIntVector child = parent.addOrGet(name, FieldTypes.INT8, TinyIntVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addUInt2VectorAsChild(StructVector parent, String name, NullableUInt2Holder holder) {
        UInt2Vector child = parent.addOrGet(name, FieldTypes.UINT16, UInt2Vector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addSmallIntVectorAsChild(StructVector parent, String name, NullableSmallIntHolder holder) {
        SmallIntVector child = parent.addOrGet(name, FieldTypes.INT16, SmallIntVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addUInt4VectorAsChild(StructVector parent, String name, NullableUInt4Holder holder) {
        UInt4Vector child = parent.addOrGet(name, FieldTypes.UINT32, UInt4Vector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addIntVectorAsChild(StructVector parent, String name, NullableIntHolder holder) {
        IntVector child = parent.addOrGet(name, FieldTypes.INT32, IntVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addUInt8VectorAsChild(StructVector parent, String name, NullableUInt8Holder holder) {
        UInt8Vector child = parent.addOrGet(name, FieldTypes.UINT64, UInt8Vector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addBigIntVectorAsChild(StructVector parent, String name, NullableBigIntHolder holder) {
        BigIntVector child = parent.addOrGet(name, FieldTypes.INT64, BigIntVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    /*----------------------------------------------------------------*
     |                                                                |
     |          Floating point types                                  |
     |                                                                |
     *----------------------------------------------------------------*/

    public static void addFloat4VectorAsChild(StructVector parent, String name, NullableFloat4Holder holder) {
        Float4Vector child = parent.addOrGet(name, FieldTypes.FLOAT, Float4Vector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addFloat8VectorAsChild(StructVector parent, String name, NullableFloat8Holder holder) {
        Float8Vector child = parent.addOrGet(name, FieldTypes.DOUBLE, Float8Vector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    /*----------------------------------------------------------------*
     |                                                                |
     |          Date types                                            |
     |                                                                |
     *----------------------------------------------------------------*/

    public static void addDateDayVectorAsChild(StructVector parent, String name, NullableDateDayHolder holder) {
        DateDayVector child = parent.addOrGet(name, FieldTypes.DATE32, DateDayVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addDateMilliVectorAsChild(StructVector parent, String name, NullableDateMilliHolder holder) {
        DateMilliVector child = parent.addOrGet(name, FieldTypes.DATE64, DateMilliVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    /*----------------------------------------------------------------*
     |                                                                |
     |          Interval types                                        |
     |                                                                |
     *----------------------------------------------------------------*/

    public static void addIntervalYearVectorAsChild(StructVector parent, String name, NullableIntervalYearHolder holder) {
        IntervalYearVector child = parent.addOrGet(name, FieldTypes.INTERVAL_MONTHS, IntervalYearVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addIntervalDayVectorAsChild(StructVector parent, String name, NullableIntervalDayHolder holder) {
        IntervalDayVector child = parent.addOrGet(name, FieldTypes.INTERVAL_DAY_TIME, IntervalDayVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    /*----------------------------------------------------------------*
     |                                                                |
     |          Binary types                                          |
     |                                                                |
     *----------------------------------------------------------------*/

    public static void addVarBinaryVectorAsChild(StructVector parent, String name, NullableVarBinaryHolder holder) {
        VarBinaryVector child = parent.addOrGet(name, FieldTypes.BINARY, VarBinaryVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addFixedSizeBinaryVectorAsChild(StructVector parent, String name, NullableFixedSizeBinaryHolder holder) {
        FixedSizeBinaryVector child = parent.addOrGet(name, FieldTypes.FIXED_SIZE_BINARY(holder.byteWidth), FixedSizeBinaryVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addLargeVarBinaryVectorAsChild(StructVector parent, String name, NullableLargeVarBinaryHolder holder) {
        LargeVarBinaryVector child = parent.addOrGet(name, FieldTypes.LARGE_BINARY, LargeVarBinaryVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    /*----------------------------------------------------------------*
     |                                                                |
     |          String types                                          |
     |                                                                |
     *----------------------------------------------------------------*/

    public static void addVarCharVectorAsChild(StructVector parent, String name, NullableVarCharHolder holder) {
        VarCharVector child = parent.addOrGet(name, FieldTypes.STRING, VarCharVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addLargeVarCharVectorAsChild(StructVector parent, String name, NullableLargeVarCharHolder holder) {
        LargeVarCharVector child = parent.addOrGet(name, FieldTypes.LARGE_STRING, LargeVarCharVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    /*----------------------------------------------------------------*
     |                                                                |
     |          Bool type                                             |
     |                                                                |
     *----------------------------------------------------------------*/

    public static void addBitVectorAsChild(StructVector parent, String name, NullableBitHolder holder) {
        BitVector child = parent.addOrGet(name, FieldTypes.BOOL, BitVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    /*----------------------------------------------------------------*
     |                                                                |
     |          TimeStamp types                                       |
     |                                                                |
     *----------------------------------------------------------------*/

    public static void addTimeStampMicroTZVectorAsChild(StructVector parent, String name, NullableTimeStampMicroTZHolder holder) {
        TimeStampMicroTZVector child = parent.addOrGet(name, 
            FieldTypes.TIMESTAMP(TimeUnit.MICROSECOND, holder.timezone), TimeStampMicroTZVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addTimeStampMicroVectorAsChild(StructVector parent, String name, NullableTimeStampMicroHolder holder) {
        TimeStampMicroVector child = parent.addOrGet(name, 
            FieldTypes.TIMESTAMP(TimeUnit.MICROSECOND, null), TimeStampMicroVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addTimeStampMilliTZVectorAsChild(StructVector parent, String name, NullableTimeStampMilliTZHolder holder) {
        TimeStampMilliTZVector child = parent.addOrGet(name, 
            FieldTypes.TIMESTAMP(TimeUnit.MILLISECOND, holder.timezone), TimeStampMilliTZVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addTimeStampMilliVectorAsChild(StructVector parent, String name, NullableTimeStampMilliHolder holder) {
        TimeStampMilliVector child = parent.addOrGet(name, 
            FieldTypes.TIMESTAMP(TimeUnit.MILLISECOND, null), TimeStampMilliVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addTimeStampNanoTZVectorAsChild(StructVector parent, String name, NullableTimeStampNanoTZHolder holder) {
        TimeStampNanoTZVector child = parent.addOrGet(name, 
            FieldTypes.TIMESTAMP(TimeUnit.NANOSECOND, holder.timezone), TimeStampNanoTZVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addTimeStampNanoVectorAsChild(StructVector parent, String name, NullableTimeStampNanoHolder holder) {
        TimeStampNanoVector child = parent.addOrGet(name, 
            FieldTypes.TIMESTAMP(TimeUnit.NANOSECOND, null), TimeStampNanoVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addTimeStampSecTZVectorAsChild(StructVector parent, String name, NullableTimeStampSecTZHolder holder) {
        TimeStampSecTZVector child = parent.addOrGet(name, 
            FieldTypes.TIMESTAMP(TimeUnit.SECOND, holder.timezone), TimeStampSecTZVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }

    public static void addTimeStampSecVectorAsChild(StructVector parent, String name, NullableTimeStampSecHolder holder) {
        TimeStampSecVector child = parent.addOrGet(name, 
            FieldTypes.TIMESTAMP(TimeUnit.SECOND, null), TimeStampSecVector.class);
        child.allocateNew();
        child.setSafe(0, holder);
        child.setValueCount(1);
    }
}