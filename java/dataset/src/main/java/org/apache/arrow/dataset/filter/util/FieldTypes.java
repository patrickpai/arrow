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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public final class FieldTypes {

    // Integer types
    public static final FieldType UINT8 = FieldType.nullable(new ArrowType.Int(8, false));
    public static final FieldType INT8 = FieldType.nullable(new ArrowType.Int(8, true));
    public static final FieldType UINT16 = FieldType.nullable(new ArrowType.Int(16, false));
    public static final FieldType INT16 = FieldType.nullable(new ArrowType.Int(16, true));
    public static final FieldType UINT32 = FieldType.nullable(new ArrowType.Int(32, false));
    public static final FieldType INT32 = FieldType.nullable(new ArrowType.Int(32, true));
    public static final FieldType UINT64 = FieldType.nullable(new ArrowType.Int(64, false));
    public static final FieldType INT64 = FieldType.nullable(new ArrowType.Int(64, true));

    // TODO: HALF_FLOAT is currently NOT used
    // Floating point types
    public static final FieldType HALF_FLOAT =
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF));
    public static final FieldType FLOAT = 
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
    public static final FieldType DOUBLE =
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));

    // Date types
    public static final FieldType DATE32 =
        FieldType.nullable(new ArrowType.Date(DateUnit.DAY));
    public static final FieldType DATE64 =
        FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND));

    // Interval types
    public static final FieldType INTERVAL_MONTHS =
        FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH));
    public static final FieldType INTERVAL_DAY_TIME =
        FieldType.nullable(new ArrowType.Interval(IntervalUnit.DAY_TIME));

    // Binary types
    public static final FieldType BINARY =
        FieldType.nullable(new ArrowType.Binary());
    public static final FieldType FIXED_SIZE_BINARY(int byteWidth) {
        return FieldType.nullable(new ArrowType.FixedSizeBinary(byteWidth));
    }
    public static final FieldType LARGE_BINARY =
        FieldType.nullable(new ArrowType.LargeBinary());

    // String types
    public static final FieldType STRING =
        FieldType.nullable(new ArrowType.Utf8());
    public static final FieldType LARGE_STRING =
        FieldType.nullable(new ArrowType.LargeUtf8());

    // Bool type
    public static final FieldType BOOL =
        FieldType.nullable(new ArrowType.Bool());

    // TimeStamp types
    public static final FieldType TIMESTAMP(TimeUnit timeUnit, String timezone) {
        return FieldType.nullable(new ArrowType.Timestamp(timeUnit, timezone));
    }
    
    // Null type
    public static final FieldType NULL = FieldType.nullable(new ArrowType.Null());
}