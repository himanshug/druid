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

import org.apache.hadoop.hive.conf.HiveConf;

public class LlapDaemonConfig
{
  private int numExecutors = 1;
  private long execBytes = 512 * 1024 * 1024L;
  private long ioBytes = 1L;
  private boolean isLlapIOEnabled = false;
  private boolean isLlapIODirect = false;

  private String workDir;
  private String llapClusterName = "druidllap";

  private HiveConf hiveConf;

  public LlapDaemonConfig()
  {
    HiveConf conf = new HiveConf();

    // To fix Class org.apache.hadoop.net.StandardSocketFactory not found
    conf.setClassLoader(getClass().getClassLoader());

    conf.set(HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, "@" + getLlapClusterName());
    conf.set(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, "localhost");
    conf.setInt(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.varname, 2181);

    this.hiveConf = conf;

    this.workDir = System.getProperty("java.io.tmpdir");
  }

  public int getNumExecutors()
  {
    return numExecutors;
  }

  public long getExecBytes()
  {
    return execBytes;
  }

  public long getIoBytes()
  {
    return ioBytes;
  }

  public boolean isLlapIOEnabled()
  {
    return isLlapIOEnabled;
  }

  public boolean isLlapIODirect()
  {
    return isLlapIODirect;
  }

  public String getWorkDir()
  {
    return workDir;
  }

  public String getLlapClusterName()
  {
    return llapClusterName;
  }

  public HiveConf getHiveConf()
  {
    return hiveConf;
  }
}
