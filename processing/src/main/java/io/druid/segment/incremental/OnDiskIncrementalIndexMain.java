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

package io.druid.segment.incremental;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public class OnDiskIncrementalIndexMain
{
  private final ObjectMapper mapper;
  private final boolean onDisk = false;

  OnDiskIncrementalIndexMain(ObjectMapper mapper)
  {
    this.mapper = mapper;
  }

  public void createIndex(
      File inputDataFile,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      QueryGranularity gran,
      int maxRowCount
  ) throws Exception
  {
    createIndex(
        new FileInputStream(inputDataFile),
        parserJson,
        aggregators,
        outDir,
        minTimestamp,
        gran,
        maxRowCount
    );
  }

  public void createIndex(
      InputStream inputDataStream,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      QueryGranularity gran,
      int maxRowCount
  ) throws Exception
  {
    try {
      StringInputRowParser parser = mapper.readValue(parserJson, StringInputRowParser.class);

      LineIterator iter = IOUtils.lineIterator(inputDataStream, "UTF-8");
      List<AggregatorFactory> aggregatorSpecs = mapper.readValue(
          aggregators,
          new TypeReference<List<AggregatorFactory>>()
          {
          }
      );

      createIndex(
          iter,
          parser,
          aggregatorSpecs.toArray(new AggregatorFactory[0]),
          outDir,
          minTimestamp,
          gran,
          true,
          maxRowCount
      );
    }
    finally {
      Closeables.close(inputDataStream, true);
    }
  }

  public void createIndex(
      Iterator rows,
      InputRowParser parser,
      final AggregatorFactory[] metrics,
      File outDir,
      long minTimestamp,
      QueryGranularity gran,
      boolean deserializeComplexMetrics,
      int maxRowCount
  ) throws Exception
  {
    IncrementalIndex index = null;
    List<File> toMerge = new ArrayList<>();

    long start = System.currentTimeMillis();

    try {
      if (onDisk) {
        index = new OnDiskIncrementalIndex(
            minTimestamp, QueryGranularity.NONE, metrics, maxRowCount,
            new StupidPool<ByteBuffer>(
                new Supplier<ByteBuffer>()
                {
                  @Override
                  public ByteBuffer get()
                  {
                    return ByteBuffer.allocateDirect(1024 * 1024 * 1024);
                  }
                }
            )
        );
      } else {
        index = new OnheapIncrementalIndex(
            minTimestamp, QueryGranularity.NONE, metrics, maxRowCount
        );
      }

      int counter = 0;
      while (rows.hasNext()) {
        if (counter++ % 500000 == 0) {
          System.out.println("counter = " + counter + ", index = " + index.size() + ", time spent = " + (System.currentTimeMillis() - start));
        }
        Object row = rows.next();
        if (row instanceof String && parser instanceof StringInputRowParser) {
          //Note: this is required because StringInputRowParser is InputRowParser<ByteBuffer> as opposed to
          //InputRowsParser<String>
          index.add(((StringInputRowParser) parser).parse((String) row));
        } else {
          index.add(parser.parse(row));
        }
      }

      System.out.println("Indexing Time = " + (System.currentTimeMillis() - start));
      start = System.currentTimeMillis();

      Iterator<Row> iter = index.iterator();
      counter = 0;
      while(iter.hasNext()) {
        if (counter++ % 500000 == 0) {
          System.out.println("counter = " + counter );
        }
        iter.next();
      }

      System.out.println("Iteration Time = " + (System.currentTimeMillis() - start));
    }
    finally {
      if (index != null) {
        index.close();
      }
    }
  }


  public static void main(String[] args) throws Exception
  {
    long start = System.currentTimeMillis();

    OnDiskIncrementalIndexMain mainObj = new OnDiskIncrementalIndexMain(new DefaultObjectMapper());

    String metricSpecJson = "[ {\n"
                            + "        \"type\" : \"count\",\n"
                            + "        \"name\" : \"count\"\n"
                            + "      }, {\n"
                            + "        \"type\" : \"longSum\",\n"
                            + "        \"name\" : \"L_QUANTITY\",\n"
                            + "        \"fieldName\" : \"L_QUANTITY\"\n"
                            + "      }, {\n"
                            + "        \"type\" : \"doubleSum\",\n"
                            + "        \"name\" : \"L_EXTENDEDPRICE\",\n"
                            + "        \"fieldName\" : \"L_EXTENDEDPRICE\"\n"
                            + "      }, {\n"
                            + "        \"type\" : \"doubleSum\",\n"
                            + "        \"name\" : \"L_DISCOUNT\",\n"
                            + "        \"fieldName\" : \"L_DISCOUNT\"\n"
                            + "      }, {\n"
                            + "        \"type\" : \"doubleSum\",\n"
                            + "        \"name\" : \"L_TAX\",\n"
                            + "        \"fieldName\" : \"L_TAX\"\n"
                            + "      } ]";



    String parseSpecJson = "{\n"
                           + "        \"type\" : \"string\",\n"
                           + "        \"parseSpec\" : {\n"
                           + "          \"format\" : \"tsv\",\n"
                           + "          \"timestampSpec\" : {\n"
                           + "            \"column\" : \"l_shipdate\",\n"
                           + "            \"format\" : \"yyyy-MM-dd\"\n"
                           + "          },\n"
                           + "          \"dimensionsSpec\" : {\n"
                           + "            \"dimensions\" : [ \"l_orderkey\", \"l_partkey\", \"l_suppkey\", \"l_linenumber\", \"l_returnflag\", \"l_linestatus\", \"l_commitdate\", \"l_receiptdate\", \"l_shipinstruct\", \"l_shipmode\", \"l_comment\" ],\n"
                           + "            \"dimensionExclusions\" : [ \"L_TAX\", \"L_EXTENDEDPRICE\", \"L_DISCOUNT\", \"count\", \"l_shipdate\", \"L_QUANTITY\" ],\n"
                           + "            \"spatialDimensions\" : [ ]\n"
                           + "          },\n"
                           + "          \"delimiter\" : \"|\",\n"
                           + "          \"listDelimiter\" : null,\n"
                           + "          \"columns\" : [ \"l_orderkey\", \"l_partkey\", \"l_suppkey\", \"l_linenumber\", \"l_quantity\", \"l_extendedprice\", \"l_discount\", \"l_tax\", \"l_returnflag\", \"l_linestatus\", \"l_shipdate\", \"l_commitdate\", \"l_receiptdate\", \"l_shipinstruct\", \"l_shipmode\", \"l_comment\" ]\n"
                           + "        }\n"
                           + "      }";

    /*
      File inputDataFile,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      QueryGranularity gran,
      int maxRowCount
     */
    mainObj.createIndex(
        new File("/Users/himanshg/work/druid/stuff/lineitem/extracted/lineitem.tbl"),
        parseSpecJson,
        metricSpecJson,
        null, //TODO: persist dir
        0,
        QueryGranularity.NONE,
        1000000000
    );


  }
}
