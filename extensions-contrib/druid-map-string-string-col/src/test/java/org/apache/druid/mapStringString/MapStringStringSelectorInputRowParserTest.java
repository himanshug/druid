/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.mapStringString;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class MapStringStringSelectorInputRowParserTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.registerModules(new MapStringStringDruidModule().getJacksonModules());

    MapStringStringSelectorInputRowParser parser = new MapStringStringSelectorInputRowParser(
        new StringInputRowParser(new TimeAndDimsParseSpec(null, null), null),
        "tags",
        Sets.newHashSet("a", "b")
    );

    System.out.println(jsonMapper.writeValueAsString(parser));

    MapStringStringSelectorInputRowParser deserializedParser = (MapStringStringSelectorInputRowParser) jsonMapper.readValue(
        jsonMapper.writeValueAsString(parser),
        InputRowParser.class
    );

    Assert.assertEquals("tags", deserializedParser.getMapColumnName());
    Assert.assertTrue(Sets.newHashSet("a", "b").equals(deserializedParser.getTopLevelColumnKeys()));
    Assert.assertTrue(deserializedParser.getDelegate() instanceof StringInputRowParser);
  }

  //@Test
  public void testSerde2() throws Exception
  {
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.registerModules(new MapStringStringDruidModule().getJacksonModules());
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, jsonMapper));
    jsonMapper.registerSubtypes(
        new NamedType(LocalFirehoseFactory.class, "local")
    );
    jsonMapper.registerModules(new FirehoseModule().getJacksonModules());

    jsonMapper.registerSubtypes(
        new NamedType(ParallelIndexTuningConfig.class, "index_parallel")
    );

    ParallelIndexSupervisorTask task = (ParallelIndexSupervisorTask) jsonMapper.readValue(
        new File("/Users/hgupta/work/druid/examples/quickstart/tutorial/wikipedia-index.json"),
        ParallelIndexSupervisorTask.class
    );

    System.out.println(jsonMapper.writeValueAsString(task));
  }
}
