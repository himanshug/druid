/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.hdfs;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.multibindings.MapBinder;
import com.metamx.common.logger.Logger;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogs;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 */
public class HdfsStorageDruidModule implements DruidModule
{
  private static final Logger LOG = new Logger(HdfsStorageDruidModule.class);

  public static final String SCHEME = "hdfs";
  private static final Configuration HADOOP_CONF;

  private Properties props = null;

  static {
    LOG.info("Setting up Hdfs Storage Module with classloader used for loading the class");

    HADOOP_CONF = new Configuration();
    HADOOP_CONF.setClassLoader(HdfsStorageDruidModule.class.getClassLoader());

    // Ensure that FileSystem class level initialization happens with correct CL
    // See https://github.com/druid-io/druid/issues/1714
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(HdfsStorageDruidModule.class.getClassLoader());
      FileSystem.get(HADOOP_CONF);
    }
    catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Inject
  public void setProperties(Properties props)
  {
    this.props = props;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "DruidHDFSStorage-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(HdfsLoadSpec.class);
          }
        }
    );
  }

  @Override
  public void configure(Binder binder)
  {
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding(SCHEME)
             .to(HdfsFileTimestampVersionFinder.class)
             .in(LazySingleton.class);

    Binders.dataSegmentPullerBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentPuller.class).in(LazySingleton.class);
    Binders.dataSegmentPusherBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentPusher.class).in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentKiller.class).in(LazySingleton.class);

    if (props != null) {
      for (String propName : System.getProperties().stringPropertyNames()) {
        if (propName.startsWith("hadoop.")) {
          HADOOP_CONF.set(propName.substring("hadoop.".length()), System.getProperty(propName));
        }
      }
    }

    binder.bind(Configuration.class).toInstance(HADOOP_CONF);
    JsonConfigProvider.bind(binder, "druid.storage", HdfsDataSegmentPusherConfig.class);

    Binders.taskLogsBinder(binder).addBinding("hdfs").to(HdfsTaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", HdfsTaskLogsConfig.class);
    binder.bind(HdfsTaskLogs.class).in(LazySingleton.class);
  }
}
