/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.quantiles;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregationTestHelper;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 */
public class QuantilesSketchAggregationGroupByQueryTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public QuantilesSketchAggregationGroupByQueryTest()
  {
    QuantilesSketchModule sm = new QuantilesSketchModule();
    sm.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(sm.getJacksonModules(), tempFolder);
  }

  @Test
  public void testSimpleDataIngestAndGpByQuery() throws Exception
  {
    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/quantiles_test_data.tsv").getFile()),
        readFileFromClasspathAsString("quantiles/quantiles_test_data_record_parser.json"),
        readFileFromClasspathAsString("quantiles/quantiles_test_data_aggregators.json"),
        0,
        QueryGranularity.NONE,
        5,
        readFileFromClasspathAsString("quantiles/quantiles_test_data_group_by_query.json")
    );

    List results = Sequences.toList(seq, Lists.newArrayList());
    Assert.assertEquals(1, results.size());

    MapBasedRow resultRow = (MapBasedRow) Iterables.getOnlyElement(results);
    Assert.assertEquals(DateTime.parse("2014-10-19T00:00:00.000Z"), resultRow.getTimestamp());

    Map<String, Object> resultEvent = resultRow.getEvent();
    Assert.assertEquals(3360L, resultEvent.get("valueSketch"));
    Assert.assertEquals(0L, resultEvent.get("non_existing_col_validation"));
    Assert.assertEquals(100.0d, ((Double)resultEvent.get("valueMin")).doubleValue(), 0.0001);
    Assert.assertEquals(3459.0d, ((Double)resultEvent.get("valueMax")).doubleValue(), 0.0001);
    Assert.assertEquals(940.0d, ((Double)resultEvent.get("valueQuantile")).doubleValue(), 0.0001);
    Assert.assertArrayEquals(
        new double[]{100.0, 940.0, 1780.0, 2620.0, 3459.0},
        (double[]) resultEvent.get("valueQuantiles"),
        0.0001
    );
    Assert.assertArrayEquals(
        new double[]{840.0, 1680.0, 840.0},
        (double[]) resultEvent.get("valueCustomSplitsHistogram"),
        0.0001
    );
    Assert.assertArrayEquals(
        new double[]{1680.0, 1680.0},
        (double[]) resultEvent.get("valueEqualSplitsHistogram"),
        0.0001
    );
  }

  private final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(QuantilesSketchAggregationGroupByQueryTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
