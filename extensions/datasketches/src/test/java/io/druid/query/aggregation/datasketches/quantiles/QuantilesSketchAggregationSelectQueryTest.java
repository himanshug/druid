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
import io.druid.granularity.QueryGranularity;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.select.SelectResultValue;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 */
public class QuantilesSketchAggregationSelectQueryTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public QuantilesSketchAggregationSelectQueryTest()
  {
    QuantilesSketchModule sm = new QuantilesSketchModule();
    sm.configure(null);
    helper = AggregationTestHelper.createSelectQueryAggregationTestHelper(
        sm.getJacksonModules(),
        tempFolder
    );
  }

  @Test
  public void testSimpleDataIngestAndSelectQuery() throws Exception
  {
    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/quantiles_test_data.tsv").getFile()),
        readFileFromClasspathAsString("quantiles/quantiles_test_data_record_parser.json"),
        readFileFromClasspathAsString("quantiles/quantiles_test_data_aggregators.json"),
        0,
        QueryGranularity.NONE,
        5000,
        readFileFromClasspathAsString("select_query.json")
    );

    Result<SelectResultValue> result = (Result<SelectResultValue>) Iterables.getOnlyElement(
        Sequences.toList(
            seq,
            Lists.newArrayList()
        )
    );
    Assert.assertEquals(new DateTime("2014-10-20T00:00:00.000Z"), result.getTimestamp());
    Assert.assertEquals(100, result.getValue().getEvents().size());
    Assert.assertEquals(
        "BQEIAABAAAABAAAAAAAAAAAAAAAAAFlAAAAAAAAAWUAEAAAAAAAAAAAAAAAAAFlAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        result.getValue().getEvents().get(0).getEvent().get("value")
    );
  }

  private final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(QuantilesSketchAggregationSelectQueryTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
