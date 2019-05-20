package org.apache.druid.discovery.etcd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.guice.annotations.Json;

public class K8sDruidNodeAnnouncer implements DruidNodeAnnouncer
{
  private final ObjectMapper jsonMapper;

  @Inject
  public K8sDruidNodeAnnouncer(@Json ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void announce(DiscoveryDruidNode discoveryDruidNode)
  {
    // Add following annotation to my pod.
    // clusterName : <unique cluster name>
    // nodeType : json-serialized(discoveryDruidNode)
  }

  @Override
  public void unannounce(DiscoveryDruidNode discoveryDruidNode)
  {
    // Remove annotations added in announce.
  }
}
