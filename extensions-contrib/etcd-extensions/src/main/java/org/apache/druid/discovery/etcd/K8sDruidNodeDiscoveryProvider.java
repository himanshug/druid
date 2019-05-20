package org.apache.druid.discovery.etcd;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.proto.V1;
import io.kubernetes.client.util.Watch;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeType;

import javax.inject.Inject;

public class K8sDruidNodeDiscoveryProvider extends DruidNodeDiscoveryProvider
{
  private final ApiClient k8sClient;

  @Inject
  public K8sDruidNodeDiscoveryProvider(ApiClient k8sClient)
  {
    this.k8sClient = k8sClient;
  }

  @Override
  public DruidNodeDiscovery getForNodeType(NodeType nodeType)
  {
    CoreV1Api api = new CoreV1Api();

    Watch<V1.EndpointAddress> watch = Watch.createWatch(
        k8sClient,
        api.createNamespacedEndpointsCall(),

    )
    return null;
  }
}
