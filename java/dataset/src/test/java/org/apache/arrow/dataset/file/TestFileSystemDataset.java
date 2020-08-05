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

package org.apache.arrow.dataset.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.file.ParquetWriteSupport.GenericRecordListBuilder;
import org.apache.arrow.dataset.filter.Expression;
import org.apache.arrow.dataset.filter.FieldExpression;
import org.apache.arrow.dataset.filter.binary.AndExpression;
import org.apache.arrow.dataset.filter.binary.ComparisonExpression;
import org.apache.arrow.dataset.filter.binary.OrExpression;
import org.apache.arrow.dataset.filter.scalar.BoolScalarExpression;
import org.apache.arrow.dataset.filter.scalar.fp.DoubleScalarExpression;
import org.apache.arrow.dataset.filter.scalar.fp.FloatScalarExpression;
import org.apache.arrow.dataset.filter.scalar.integers.Int32ScalarExpression;
import org.apache.arrow.dataset.filter.unary.InExpression;
import org.apache.arrow.dataset.filter.unary.NotExpression;
import org.apache.arrow.dataset.filter.util.Util;
import org.apache.arrow.dataset.jni.NativeDataset;
import org.apache.arrow.dataset.jni.NativeInstanceReleasedException;
import org.apache.arrow.dataset.jni.NativeScanTask;
import org.apache.arrow.dataset.jni.NativeScanner;
import org.apache.arrow.dataset.jni.TestNativeDataset;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.flatbuf.CompareOperator;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TestFileSystemDataset extends TestNativeDataset {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  public static final String AVRO_SCHEMA_USER = "user.avsc";
  public static final String AVRO_SCHEMA_AB = "ab.avsc";
  public static final String AVRO_SCHEMA_S = "s.avsc";
  public static final String AVRO_SCHEMA_DATE = "date.avsc";

  @Test
  public void testParquetRead() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(new String[0], 100);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);

    assertSingleTaskProduced(factory, options);
    assertEquals(1, datum.size());
    assertEquals(2, schema.getFields().size());
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals("name", schema.getFields().get(1).getName());
    assertEquals(Types.MinorType.INT.getType(), schema.getFields().get(0).getType());
    assertEquals(Types.MinorType.VARCHAR.getType(), schema.getFields().get(1).getType());
    checkParquetReadResult(schema, writeSupport.getWrittenRecords(), datum);

    AutoCloseables.close(datum);
  }

  @Test
  public void testParquetProjector() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(new String[]{"id"}, 100);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    org.apache.avro.Schema expectedSchema = truncateAvroSchema(writeSupport.getAvroSchema(), 0, 1);

    assertSingleTaskProduced(factory, options);
    assertEquals(1, schema.getFields().size());
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals(Types.MinorType.INT.getType(), schema.getFields().get(0).getType());
    assertEquals(1, datum.size());
    checkParquetReadResult(schema,
        Collections.singletonList(
            new GenericRecordBuilder(
                expectedSchema)
                .set("id", 1)
                .build()), datum);

    AutoCloseables.close(datum);
  }

  @Test
  public void testParquetBatchSize() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(),
        1, "a", 2, "b", 3, "c");

    ScanOptions options = new ScanOptions(new String[0], 1);
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);

    assertSingleTaskProduced(factory, options);
    assertEquals(3, datum.size());
    datum.forEach(batch -> assertEquals(1, batch.getLength()));
    checkParquetReadResult(schema, writeSupport.getWrittenRecords(), datum);

    AutoCloseables.close(datum);
  }

  @Test
  public void testTrivial() throws Exception {
    Integer[] a = new Integer[] {0, 0, 1, 2, 0, 0, 0};
    Double[] b = new Double[] {-0.1, 0.3, 0.2, -0.1, 0.1, null, 1.0};
    boolean[] in = new boolean[] {true, true, true, true, true, true, true};

    Object[] recordsToWrite = getRowMajorOrder(a, b);
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_AB, TMP.newFolder(), recordsToWrite);
    
    Object[] expectedValuesWritten = getExpectedValuesWritten(in, a, b);
    List<GenericRecord> expectedRecords = writeSupport.getRecordListBuilder().createRecordList(expectedValuesWritten);

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
    
    BitHolder bitHolder = new BitHolder();
    bitHolder.value = 1;

    BoolScalarExpression expr = new BoolScalarExpression(bitHolder);

    StructVector vector = expr.toVector("root", rootAllocator());
    VectorSchemaRoot vectorSchemaRoot = Util.loadIntoVectorSchemaRoot(vector);

    ScanOptions options = new ScanOptions(new String[0], 100, vectorSchemaRoot);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    checkParquetReadResult(schema, expectedRecords, datum);

    AutoCloseables.close(expr);
    AutoCloseables.close(vector, vectorSchemaRoot);
    AutoCloseables.close(datum);
  }

  @Test
  public void testTrivial2() throws Exception {
    Integer[] a = new Integer[] {0, 0, 1, 2, 0, 0, 0};
    Double[] b = new Double[] {-0.1, 0.3, 0.2, -0.1, 0.1, null, 1.0};
    boolean[] in = new boolean[] {false, false, false, false, false, false, false};

    Object[] recordsToWrite = getRowMajorOrder(a, b);
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_AB, TMP.newFolder(), recordsToWrite);
    
    Object[] expectedValuesWritten = getExpectedValuesWritten(in, a, b);
    List<GenericRecord> expectedRecords = writeSupport.getRecordListBuilder().createRecordList(expectedValuesWritten);

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
    
    BitHolder bitHolder = new BitHolder();
    bitHolder.value = 0;

    BoolScalarExpression expr = new BoolScalarExpression(bitHolder);

    StructVector vector = expr.toVector("root", rootAllocator());
    VectorSchemaRoot vectorSchemaRoot = Util.loadIntoVectorSchemaRoot(vector);

    ScanOptions options = new ScanOptions(new String[0], 100, vectorSchemaRoot);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    checkParquetReadResult(schema, expectedRecords, datum);

    AutoCloseables.close(expr);
    AutoCloseables.close(vector, vectorSchemaRoot);
    AutoCloseables.close(datum);
  }

  @Test
  public void testFilterBasics() throws Exception {
    Integer[] a = new Integer[] {0, 0, 1, 2, 0, 0, 0};
    Double[] b = new Double[] {-0.1, 0.3, 0.2, -0.1, 0.1, null, 1.0};
    boolean[] in = new boolean[] {false, true, false, false, true, false, false};

    Object[] recordsToWrite = getRowMajorOrder(a, b);
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_AB, TMP.newFolder(), recordsToWrite);
    
    Object[] expectedValuesWritten = getExpectedValuesWritten(in, a, b);
    List<GenericRecord> expectedRecords = writeSupport.getRecordListBuilder().createRecordList(expectedValuesWritten);

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
            FileFormat.PARQUET, writeSupport.getOutputURI());

    IntHolder intHolder = new IntHolder();
    Float8Holder doubleHolder = new Float8Holder();
    
    // "a" == 0
    intHolder.value = 0;
    ComparisonExpression aEq0 = new ComparisonExpression(
      new FieldExpression("a"),
      CompareOperator.EQUAL,
      new Int32ScalarExpression(intHolder)
    );
    
    // "b" > 0.0
    doubleHolder.value = 0.0;
    ComparisonExpression bGt0 = new ComparisonExpression(
      new FieldExpression("b"),
      CompareOperator.GREATER,
      new DoubleScalarExpression(doubleHolder)
    );

    // "b" < 1.0
    doubleHolder.value = 1.0;
    ComparisonExpression bLt1 = new ComparisonExpression(
      new FieldExpression("b"),
      CompareOperator.LESS,
      new DoubleScalarExpression(doubleHolder)
    );

    AndExpression expr = new AndExpression(aEq0, new AndExpression(bGt0, bLt1));

    StructVector vector = expr.toVector("root", rootAllocator());
    VectorSchemaRoot vectorSchemaRoot = Util.loadIntoVectorSchemaRoot(vector);

    ScanOptions options = new ScanOptions(new String[0], 100, vectorSchemaRoot);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    checkParquetReadResult(schema, expectedRecords, datum);

    AutoCloseables.close(aEq0, bGt0, bLt1, expr);
    AutoCloseables.close(vector, vectorSchemaRoot);
    AutoCloseables.close(datum);
  }

  @Test
  public void testFilterBasics2() throws Exception {
    Integer[] a = new Integer[] {0, 0, 1, 2, 0, 0, 0};
    Double[] b = new Double[] {-0.1, 0.3, 0.2, -0.1, 0.1, null, 1.0};
    boolean[] in = new boolean[] {false, false, true, false, false, false, false};

    Object[] recordsToWrite = getRowMajorOrder(a, b);
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_AB, TMP.newFolder(), recordsToWrite);
    
    Object[] expectedValuesWritten = getExpectedValuesWritten(in, a, b);
    List<GenericRecord> expectedRecords = writeSupport.getRecordListBuilder().createRecordList(expectedValuesWritten);

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
            FileFormat.PARQUET, writeSupport.getOutputURI());

    IntHolder intHolder = new IntHolder();
    Float8Holder doubleHolder = new Float8Holder();
    
    // "a" != 0
    intHolder.value = 0;
    NotExpression aNEq0 = new NotExpression(
      new ComparisonExpression(
        new FieldExpression("a"),
        CompareOperator.EQUAL,
        new Int32ScalarExpression(intHolder)
      )
    ); 
    
    // "b" > 0.1
    doubleHolder.value = 0.1;
    ComparisonExpression bGt0p1 = new ComparisonExpression(
      new FieldExpression("b"),
      CompareOperator.GREATER,
      new DoubleScalarExpression(doubleHolder)
    );

    AndExpression expr = new AndExpression(aNEq0, bGt0p1);

    StructVector vector = expr.toVector("root", rootAllocator());
    VectorSchemaRoot vectorSchemaRoot = Util.loadIntoVectorSchemaRoot(vector);

    ScanOptions options = new ScanOptions(new String[0], 100, vectorSchemaRoot);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    checkParquetReadResult(schema, expectedRecords, datum);

    AutoCloseables.close(aNEq0, bGt0p1, expr);
    AutoCloseables.close(vector, vectorSchemaRoot);
    AutoCloseables.close(datum);
  }

  @Test
  public void testInExpression() throws Exception {
    String[] a = new String[] {"hello", "world", "", null, "foo", "hello", "bar"};
    boolean[] in = new boolean[] {true, true, false, false, false, true, false};

    Object[] recordsToWrite = getRowMajorOrder(a);
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_S, TMP.newFolder(), recordsToWrite);
    
    Object[] expectedValuesWritten = getExpectedValuesWritten(in, a);
    List<GenericRecord> expectedRecords = writeSupport.getRecordListBuilder().createRecordList(expectedValuesWritten);

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
            FileFormat.PARQUET, writeSupport.getOutputURI());
    
    FieldExpression s = new FieldExpression("s");

    VarCharVector set = new VarCharVector("set", rootAllocator());
    set.allocateNew();
    set.set(0, "hello".getBytes());
    set.set(1, "world".getBytes());
    set.setValueCount(2);

    InExpression<VarCharVector> expr = new InExpression<>(rootAllocator(), s, set);

    StructVector vector = expr.toVector("root", rootAllocator());
    VectorSchemaRoot vectorSchemaRoot = Util.loadIntoVectorSchemaRoot(vector);

    ScanOptions options = new ScanOptions(new String[0], 100, vectorSchemaRoot);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    checkParquetReadResult(schema, expectedRecords, datum);

    AutoCloseables.close(s, set, expr);
    AutoCloseables.close(vector, vectorSchemaRoot);
    AutoCloseables.close(datum);
  }

  @Test
  public void testIntegerTypes() throws Exception {
    /**
     *  year    month    day    hour    alpha    beta    gamma    in
     *  1999    12       31     0       0        3.25    94023    true
     *  1999    12       31     0       0        5.25    42982    true
     *  1999    12       31     0       5        3.25    74382    true
     *  1999    12       31     5       0        3.25    47980    false
     *  1999    12       5      0       0        3.25    32901    false
     *  1999    5        31     0       0        3.25    45901    false
     *  1995    12       31     0       0        3.25    95329    false
     *  2020    8        5      9       29       9.99    57282    false
     *  1999    12       31     0       0        3.25    29193    true 
     */
    Short[] year = new Short[] {1999, 1999, 1999, 1999, 1999, 1999, 1995, 2020, 1999};
    Integer[] month = new Integer[] {12, 12, 12, 12, 12, 5, 12, 8, 12};
    Integer[] day = new Integer[] {31, 31, 31, 31, 5, 31, 31, 5, 31};
    Integer[] hour = new Integer[] {0, 0, 0, 5, 0, 0, 0, 9, 0};
    Integer[] alpha = new Integer[] {0, 0, 5, 0, 0, 0, 0, 29, 0};
    Float[] beta = new Float[] {3.25f, 5.25f, 3.25f, 3.25f, 3.25f, 3.25f, 3.25f, 9.99f, 3.25f};
    Long[] gamma = new Long[] {94023l, 42982l, 74382l, 47980l, 32901l, 45901l, 95329l, 57282l, 29193l};
    boolean[] in = new boolean[] {true, true, true, false, false, false, false, false, true};

    Object[] recordsToWrite = getRowMajorOrder(year, month, day, hour, alpha, beta, gamma);
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_DATE, TMP.newFolder(), recordsToWrite);
    
    Object[] expectedValuesWritten = getExpectedValuesWritten(in, year, month, day, hour, alpha, beta, gamma);
    List<GenericRecord> expectedRecords = writeSupport.getRecordListBuilder().createRecordList(expectedValuesWritten);

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
            FileFormat.PARQUET, writeSupport.getOutputURI());

    IntHolder intHolder = new IntHolder();
    Float4Holder floatHolder = new Float4Holder();
    
    // "year" == int16_t(1999)
    intHolder.value = 1999;
    ComparisonExpression yearEq1999 = new ComparisonExpression(
      new FieldExpression("year"),
      CompareOperator.EQUAL,
      new Int32ScalarExpression(intHolder)
    );
    
    // "month" == int8_t(12)
    intHolder.value = 12;
    ComparisonExpression monthEq12 = new ComparisonExpression(
      new FieldExpression("month"),
      CompareOperator.EQUAL,
      new Int32ScalarExpression(intHolder)
    );

    // "day" == int8_t(31)
    intHolder.value = 31;
    ComparisonExpression dayEq31 = new ComparisonExpression(
      new FieldExpression("day"),
      CompareOperator.EQUAL,
      new Int32ScalarExpression(intHolder)
    );

    // "hour" == int8_t(0)
    intHolder.value = 0;
    ComparisonExpression hourEq0 = new ComparisonExpression(
      new FieldExpression("hour"),
      CompareOperator.EQUAL,
      new Int32ScalarExpression(intHolder)
    );

    // "alpha" == int32_t(0)
    intHolder.value = 0;
    ComparisonExpression alphaEq0 = new ComparisonExpression(
      new FieldExpression("alpha"),
      CompareOperator.EQUAL,
      new Int32ScalarExpression(intHolder)
    );

    // "beta" == 3.25f
    floatHolder.value = 3.25f;
    ComparisonExpression betaEq3p25f = new ComparisonExpression(
      new FieldExpression("beta"),
      CompareOperator.EQUAL,
      new FloatScalarExpression(floatHolder)
    );

    AndExpression expr = new AndExpression(
      new AndExpression(
        new AndExpression(
          yearEq1999,
          monthEq12
        ),
        new AndExpression(
          dayEq31, 
          hourEq0
        )
      ),
      new OrExpression(
        alphaEq0,
        betaEq3p25f
      )
    );

    StructVector vector = expr.toVector("root", rootAllocator());
    VectorSchemaRoot vectorSchemaRoot = Util.loadIntoVectorSchemaRoot(vector);

    ScanOptions options = new ScanOptions(new String[0], 100, vectorSchemaRoot);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    checkParquetReadResult(schema, expectedRecords, datum);

    AutoCloseables.close(yearEq1999, monthEq12, dayEq31, hourEq0, alphaEq0, betaEq3p25f, expr);
    AutoCloseables.close(vector, vectorSchemaRoot);
    AutoCloseables.close(datum);
  }

  @Test
  public void testCloseAgain() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, writeSupport.getOutputURI());

    assertDoesNotThrow(() -> {
      NativeDataset dataset = factory.finish();
      dataset.close();
      dataset.close();
    });
  }

  @Test
  public void testScanAgain() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(new String[0], 100);
    NativeScanner scanner = dataset.newScan(options);
    List<? extends NativeScanTask> taskList1 = collect(scanner.scan());
    List<? extends NativeScanTask> taskList2 = collect(scanner.scan());
    NativeScanTask task1 = taskList1.get(0);
    NativeScanTask task2 = taskList2.get(0);
    List<ArrowRecordBatch> datum = collect(task1.execute());

    UnsupportedOperationException uoe = assertThrows(UnsupportedOperationException.class, task2::execute);
    Assertions.assertEquals("NativeScanner cannot be executed more than once. Consider creating new scanner instead",
        uoe.getMessage());

    AutoCloseables.close(datum);
    AutoCloseables.close(taskList1);
    AutoCloseables.close(taskList2);
    AutoCloseables.close(scanner, dataset, factory);
  }

  @Test
  public void testScanAfterClose1() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(new String[0], 100);
    NativeScanner scanner = dataset.newScan(options);
    scanner.close();
    assertThrows(NativeInstanceReleasedException.class, scanner::scan);
  }

  @Test
  public void testScanAfterClose2() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(new String[0], 100);
    NativeScanner scanner = dataset.newScan(options);
    List<? extends NativeScanTask> tasks = collect(scanner.scan());
    NativeScanTask task = tasks.get(0);
    task.close();
    assertThrows(NativeInstanceReleasedException.class, task::execute);
  }

  @Test
  public void testScanAfterClose3() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");

    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, writeSupport.getOutputURI());
    NativeDataset dataset = factory.finish();
    ScanOptions options = new ScanOptions(new String[0], 100);
    NativeScanner scanner = dataset.newScan(options);
    List<? extends NativeScanTask> tasks = collect(scanner.scan());
    NativeScanTask task = tasks.get(0);
    ScanTask.BatchIterator iterator = task.execute();
    task.close();
    assertThrows(NativeInstanceReleasedException.class, iterator::hasNext);
  }

  private void checkParquetReadResult(Schema schema, List<GenericRecord> expected, List<ArrowRecordBatch> actual) {
    assertEquals(expected.size(), actual.stream()
        .mapToInt(ArrowRecordBatch::getLength)
        .sum());
    final int fieldCount = schema.getFields().size();
    LinkedList<GenericRecord> expectedRemovable = new LinkedList<>(expected);
    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, rootAllocator())) {
      VectorLoader loader = new VectorLoader(vsr);
      for (ArrowRecordBatch batch : actual) {
        try {
          assertEquals(fieldCount, batch.getNodes().size());
          loader.load(batch);
          int batchRowCount = vsr.getRowCount();
          for (int i = 0; i < fieldCount; i++) {
            FieldVector vector = vsr.getVector(i);
            for (int j = 0; j < batchRowCount; j++) {
              Object object = vector.getObject(j);
              Object expectedObject = expectedRemovable.get(j).get(i);
              assertEquals(Objects.toString(expectedObject),
                  Objects.toString(object));
            }
          }
          for (int i = 0; i < batchRowCount; i++) {
            expectedRemovable.poll();
          }
        } finally {
          batch.close();
        }
      }
      assertTrue(expectedRemovable.isEmpty());
    }
  }

  private org.apache.avro.Schema truncateAvroSchema(org.apache.avro.Schema schema, int from, int to) {
    List<org.apache.avro.Schema.Field> fields = schema.getFields().subList(from, to);
    return org.apache.avro.Schema.createRecord(
        fields.stream()
            .map(f -> new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
            .collect(Collectors.toList()));
  }

  /**
   * Given a list of columns, returns the row-major order.
   * 
   * Example:
   * [[1, 2, 3], ["a", "b", "c"]] -> [1, "a", 2, "b", 3, "c"]
   * 
   * Assumptions:
   *   columns is non-null
   *   columns.length > 0
   *   every column has the same length
   */
  private Object[] getRowMajorOrder(Object[]... columns) {
    int numRows = columns[0].length;
    int numColumns = columns.length;

    Object[] result = new Object[numRows * numColumns];
    int resultIdx = 0;

    int row = 0;
    while (row < numRows) {
      int column = 0;
      while (column < numColumns) {
        result[resultIdx++] = columns[column][row];
        column++;
      }
      row++;
    }

    return result;
  }

  /**
   * Returns expected values written (in row-major order) after applying a filter.
   * Row i is expected to be written if and only if in[i] is true.
   * 
   * Assumptions:
   *   in.length == columns[0].length if columns.length > 0
   *   every column has the same length
   */
  private Object[] getExpectedValuesWritten(boolean[] in, Object[]... columns) {
    int numExpectedRecords = 0;
    for (boolean included : in) {
      if (included) {
        numExpectedRecords++;
      }
    }

    int numColumns = columns.length;
    Object[] values = new Object[numExpectedRecords * numColumns];
    int valuesIdx = 0;

    for (int row = 0; row < in.length; row++) {
      if (in[row]) {
        // Copy all values in this row into 'values'
        for (int col = 0; col < numColumns; col++) {
          values[valuesIdx++] = columns[col][row];
        }
      }
    }

    return values;
  }
}
