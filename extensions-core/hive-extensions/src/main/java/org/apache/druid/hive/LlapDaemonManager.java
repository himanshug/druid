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

package org.apache.druid.hive;

import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapDaemonInfo;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;

import java.io.File;

@ManageLifecycle
public class LlapDaemonManager
{
  private static final Logger LOGGER = new Logger(LlapDaemonManager.class);

  private final LlapDaemonConfig llapDaemonConfig;
  private final LlapDaemon llapDaemon;

  @Inject
  private LlapDaemonManager(LlapDaemonConfig llapDaemonConfig)
  {

    this.llapDaemonConfig = llapDaemonConfig;

    LlapDaemonInfo.initialize(
        llapDaemonConfig.getLlapClusterName(),
        llapDaemonConfig.getNumExecutors(),
        llapDaemonConfig.getExecBytes(),
        llapDaemonConfig.getIoBytes(),
        llapDaemonConfig.isLlapIODirect(),
        llapDaemonConfig.isLlapIOEnabled(),
        "-1"
    );

    HiveConf conf = llapDaemonConfig.getHiveConf();

    int rpcPort = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_DAEMON_RPC_PORT);
    int mngPort = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_MANAGEMENT_RPC_PORT);
    int shufflePort = conf.getInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, ShuffleHandler.DEFAULT_SHUFFLE_PORT);
    int webPort = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_DAEMON_WEB_PORT);

    // TODO: do we need to clean it up every time we are restarted?
    File localDir = new File(llapDaemonConfig.getWorkDir(), "llap");

    llapDaemon = new LlapDaemon(
        conf,
        llapDaemonConfig.getNumExecutors(),
        llapDaemonConfig.getExecBytes(),
        llapDaemonConfig.isLlapIOEnabled(),
        llapDaemonConfig.isLlapIODirect(),
        llapDaemonConfig.getIoBytes(),
        new String[]{localDir.getAbsolutePath()},
        rpcPort,
        false,
        -1,
        mngPort,
        shufflePort,
        webPort,
        llapDaemonConfig.getLlapClusterName()
    );
  }
  
  @LifecycleStart
  public void start()
  {
    LOGGER.info("Starting........");

    llapDaemon.init(new Configuration(llapDaemonConfig.getHiveConf()));
    llapDaemon.start();
    
    LOGGER.info("Started.");

  }

  @LifecycleStop
  public void stop()
  {
    LOGGER.info("Stopping.......");
    
    llapDaemon.stop();
    
    LOGGER.info("Stopped.");
  }
}
