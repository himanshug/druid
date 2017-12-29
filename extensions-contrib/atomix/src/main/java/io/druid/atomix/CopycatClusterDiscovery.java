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

package io.druid.atomix;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.SequenceInputStreamResponseHandler;
import io.atomix.catalyst.transport.Address;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.common.StreamUtils;
import io.druid.java.util.common.logger.Logger;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * (1) we use a VIP, call the VIP got get copycat server addresses.
 * (2) community might not have VIP, so it should be possible to specify multiple endpoints that so that users can
 * just list a subset of coordinators. Or, should we let the specify copycate server addresses in runtime properties file?
 * (3) CopycatClient at coordinator should just use local CopycatServer address
 *
 * ideally have, different implementations and user should be able to choose in runtime properties
 * druid.atomix.bootstrapServerDiscovery.type=url/fixed
 */
public interface CopycatClusterDiscovery
{
  List<Address> getCopycatServers();
}

class VIPBasedCopycatClusterDiscovery implements CopycatClusterDiscovery
{

  private static final Logger LOG = new Logger(VIPBasedCopycatClusterDiscovery.class);

  private static final TypeReference<List<String>> LIST_OF_STRINGS_TYPE = new TypeReference<List<String>>()
  {
  };

  private URL url;
  private HttpClient httpClient;
  private Duration timeout;
  private ObjectMapper jsonMapper;

  @Override
  public List<Address> getCopycatServers()
  {
    final AtomicInteger returnCode = new AtomicInteger(0);
    final AtomicReference<String> reasonString = new AtomicReference<>(null);

    try (final InputStream result = httpClient.go(
        new Request(HttpMethod.GET, url),
        makeResponseHandler(returnCode, reasonString),
        timeout
    ).get()) {
      if (returnCode.get() == 200) {
        //TODO: debug log the response received from vip
        //what about the parsing or read errors here?
        List<String> serversList = jsonMapper.readValue(result, LIST_OF_STRINGS_TYPE);
        return serversList.stream().map(Address::new).collect(Collectors.toList());
      } else {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          StreamUtils.copyAndClose(result, baos);
        }
        catch (IOException e2) {
          LOG.warn(e2, "Error reading response from [%s]", url);
        }

        throw new IOException(
            String.format(
                "Bad response from [%s], [%d] : [%s]  Response: [%s]",
                url,
                returnCode.get(),
                reasonString.get(),
                StringUtils.fromUtf8(baos.toByteArray())
            )
        );
      }
    } catch (Exception ex) {
      //TODO: handle exceptions properly
      return null;
    }
  }

  HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
      final AtomicInteger returnCode,
      final AtomicReference<String> reasonString
  )
  {
    return new SequenceInputStreamResponseHandler()
    {
      @Override
      public ClientResponse<InputStream> handleResponse(HttpResponse response)
      {
        returnCode.set(response.getStatus().getCode());
        reasonString.set(response.getStatus().getReasonPhrase());
        return super.handleResponse(response);
      }
    };
  }
}

class StaticCopycatClusterDiscovery implements CopycatClusterDiscovery
{

  private List<Address> servers;

  @Override
  public List<Address> getCopycatServers()
  {
    return null;
  }
}
