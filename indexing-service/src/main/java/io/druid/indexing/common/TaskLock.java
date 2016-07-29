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

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.Interval;

import java.util.List;

/**
 * Represents a lock held by some task. Immutable.
 */
public class TaskLock
{
  private final String groupId;
  private final String dataSource;
  private final List<String> dataSources;
  private final Interval interval;
  private final String version;

  @JsonCreator
  public TaskLock(
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("dataSources") List<String> dataSources,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version
  )
  {
    Preconditions.checkArgument(
        dataSource == null || dataSources == null,
        "only one of dataSource or dataSources should be present"
    );

    this.groupId = groupId;
    this.dataSource = dataSource;
    this.dataSources = dataSources;
    this.interval = interval;
    this.version = version;
  }

  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<String> getDataSources()
  {
    return dataSources;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TaskLock taskLock = (TaskLock) o;

    if (!groupId.equals(taskLock.groupId)) {
      return false;
    }
    if (dataSource != null ? !dataSource.equals(taskLock.dataSource) : taskLock.dataSource != null) {
      return false;
    }
    if (dataSources != null ? !dataSources.equals(taskLock.dataSources) : taskLock.dataSources != null) {
      return false;
    }
    if (!interval.equals(taskLock.interval)) {
      return false;
    }
    return version.equals(taskLock.version);

  }

  @Override
  public int hashCode()
  {
    int result = groupId.hashCode();
    result = 31 * result + (dataSource != null ? dataSource.hashCode() : 0);
    result = 31 * result + (dataSources != null ? dataSources.hashCode() : 0);
    result = 31 * result + interval.hashCode();
    result = 31 * result + version.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "TaskLock{" +
           "groupId='" + groupId + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", dataSources=" + dataSources +
           ", interval=" + interval +
           ", version='" + version + '\'' +
           '}';
  }
}
