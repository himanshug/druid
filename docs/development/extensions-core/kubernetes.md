---
id: druid-kubernetes
title: "Kubernetes"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


Apache Druid Extension to enable using Kubernetes API Server for node discovery and leader election. This extension allows Druid cluster deployment on Kubernetes without Zookeeper.


## Configuration

To use this extension please make sure to  [include](../../development/extensions.md#loading-extensions) `druid-kubernetes-extensions` as an extension.

This extension works together with HTTP based segment and task management in Druid. Consequently, following configuration must be set on all Druid nodes.

`druid.zk.service.enabled=false`
`druid.serverview.type=http`
`druid.coordinator.loadqueuepeon.type=http`
`druid.discovery.type=k8s`

Additionally, this extension has following configuration.

### Properties
|Property|Possible Values|Description|Default|required|
|--------|---------------|-----------|-------|--------|
|`druid.discovery.k8s.clusterIdentifier`|`string that matches [a-z0-9][a-z0-9-]*[a-z0-9]`|Unique identifier for this Druid cluster in Kubernetes e.g. us-prod-druid.|None|Yes|
|`druid.discovery.k8s.podNameEnvKey`|`Pod Env Variable`|Pod Env variable whose value is that pod's name.|POD_NAME|No|
|`druid.discovery.k8s.podNamespaceEnvKey`|`Pod Env Variable`|Pod Env variable whose value is that pod's kubernetes namespace.|POD_NAMESPACE|No|
|`druid.discovery.k8s.leaseDuration`|`Duration`|Lease duration used by Leader Election algorithm. Candidates wait for this time before taking over previous Leader.|PT60S|No|
|`druid.discovery.k8s.renewDeadline`|`Duration`|Lease renewal period used by Leader.|PT17S|No|
|`druid.discovery.k8s.retryPeriod`|`Duration`|Retry wait used by Leader Election algorithm on failed operations.|PT5S|No|

