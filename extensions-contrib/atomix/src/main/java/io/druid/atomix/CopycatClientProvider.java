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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;
import io.atomix.catalyst.serializer.TypeSerializerFactory;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.Command;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.ServerSelectionStrategies;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import io.druid.server.coordination.zk.Node;

import java.io.IOException;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * TODO:
 * what happens when majority of coordinators are down
 * what happens when all coordinators are down, how does client signal its failure to application code and how would the
 * application react (probably raise an Alert and fatal error)?
 *
 * when majority/all coordinators are down, whap happens to membership and leadership listeners?
 *
 * can i provide a new list of bootstrapServers as part of recovery?
 *
 * Can we provide list of bootstrap servers again on disconnect etc.
 */
public class CopycatClientProvider
{
  private static final Logger log = new Logger(CopycatClientProvider.class);


  private final AtomixConfig config;
  private final ObjectMapper mapper;
  private final CopycatServerProvider server;
  private final String name;

  private final Queue<Pair<String, Consumer>> eventListeners = new ConcurrentLinkedQueue<>();
  private final Queue<Consumer> stateListeners = new ConcurrentLinkedQueue<>();

  volatile State thisState;
  volatile boolean stopped = false;

  private  volatile CopycatClient client;

  //discard old client if there and recreate new client
  private ExecutorService executor = null;

  //TODO: remove name param
  @Inject
  public CopycatClientProvider(String name, CopycatServerProvider server, AtomixConfig config, ObjectMapper mapper)
  {
    this.server = server;
    this.config = config;
    this.mapper = mapper;
    this.name = name;
  }

  @LifecycleStart
  public void start()
  {
    server.start();

    if (!config.isEnabled()) {
      return;
    }

    //TODO: thread name etc
    executor = Executors.newSingleThreadExecutor();

    try {
      setupNewClient().get();
    } catch (ExecutionException | InterruptedException ex) {
      log.error(ex, "Failed to start.");
      throw Throwables.propagate(ex);
    }
  }

  @LifecycleStop
  public void stop()
  {
    stopped = true;

    if (!config.isEnabled()) {
      return;
    }

    executor.shutdownNow();

    if (client != null) {
      try {
        log.info(name + "Stopping.");
        client.close().join();
        log.info(name + "Stopped.");
      } catch (Exception ex) {
        log.error(ex, name + "Failed to stop.");
      }
    }



    server.stop();
  }

  private void onStateChange(CopycatClient.State state) {
    switch(state) {
      case CONNECTED:
        log.info(name + "client is in CONNECTED state.");
        setState(State.CONNECTED);
        break;
      case SUSPENDED:
        log.warn(name + "client is in SUSPENDED state.");
        setState(State.SUSPENDED);
        break;
      case CLOSED:
        log.info(name + "client is in CLOSED state.");
        setState(State.SUSPENDED);
        if (!stopped) {
          // assumed that you can't reach here before previous future finishing up
          setupNewClient();
        }
        break;
      default:
        //TODO: this should be an alert
        log.error(name + "client is in unknown [%s] state.", state);
    }
  }

  Future setupNewClient()
  {
    log.info(name + "Setting up new client.");
    return executor.submit(
        this::newClient
    );
  }

  void newClient()
  {
    while (!Thread.interrupted()) {
      try {
        log.info(name + "Creating new client.");
        CopycatClient client = CopycatClient.builder()
                              .withTransport(new NettyTransport())
                              .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
                              //.withUnstableTimeout(Duration.ofMillis(5000)) TODO: get a new atomix build
                              .withServerSelectionStrategy(ServerSelectionStrategies.LEADER)
                              .withSessionTimeout(Duration.ofMillis(config.getSessionTimeout()))
                              .build();

        updateSerializer(client.serializer(), mapper);

        client.onStateChange(this::onStateChange);

        Address serverAddress = server.getServerAddress();
        if (serverAddress == null) {
          log.info(name + "Connecting to bootstrapServers [%s].", config.getBootstrapServers());
          client.connect(config.getBootstrapServers()).join();
        } else {
          log.info(name + "Connecting to bootstrapServers [%s].", serverAddress);
          client.connect(ImmutableList.of(serverAddress)).join();
        }
        log.info(name + "Created new client.");

        this.client = client;
        break;
      }
      catch (Exception ex) {
        log.error(ex, name + "failed to create client.");
      }

      try {
        Thread.sleep(5000); //sleep a bit between retries
      } catch (InterruptedException ex) {
        log.error(ex, "client creation thread interrupted");
        Thread.currentThread().interrupt(); //restore interrupt
      }
    }
  }

  void setState(State state)
  {
    if (this.thisState != state) {
      log.debug(name + "clientProvider set to [%s]", state);
      this.thisState = state;
      for (Consumer listener : stateListeners) {
        log.debug(name + "clientProvider calls state listener with state [%s]", state);
        listener.accept(state);
      }
    } else {
      log.debug(name + "clientProvider already set to [%s]", state);
    }
  }

  public <T> CompletableFuture<T> submit(Command<T> command)
  {
    return client.submit(command);
  }

  public synchronized <T> void onEvent(String event, Consumer<T> callback)
  {
    client.onEvent(event, callback);
    eventListeners.offer(new Pair(event, callback));
  }

  public enum State {
    CONNECTED,
    SUSPENDED
  }

  public synchronized void onStateChange(Consumer<State> callback)
  {
    stateListeners.add(callback);
  }

  public static void updateSerializer(Serializer serializer, ObjectMapper mapper)
  {
    serializer.register(
        Node.class, new TypeSerializerFactory()
        {
          @Override
          public TypeSerializer<?> createSerializer(Class<?> aClass)
          {
            return new DruidTypeSerializer(mapper);
          }
        }
    );

    serializer.register(
        GroupInfo.class, new TypeSerializerFactory()
        {
          @Override
          public TypeSerializer<?> createSerializer(Class<?> aClass)
          {
            return new DruidTypeSerializer(mapper);
          }
        }
    );

    serializer.register(
        JoinCommand.class, new TypeSerializerFactory()
        {
          @Override
          public TypeSerializer<?> createSerializer(Class<?> aClass)
          {
            return new DruidTypeSerializer(mapper);
          }
        }
    );

    serializer.register(ListenCommand.class);

    //new stuff
    serializer.register(
        DruidNode.class, (c) -> new DruidTypeSerializer(mapper)
    );

    serializer.register(
        io.druid.atomix.discovery.JoinCommand.class, (c) -> new DruidTypeSerializer(mapper)
    );

    serializer.register(
        io.druid.atomix.discovery.ListenCommand.class, (c) -> new DruidTypeSerializer(mapper)
    );
  }
}

class DruidTypeSerializer implements TypeSerializer
{
  private ObjectMapper mapper;

  public DruidTypeSerializer(@Json ObjectMapper mapper) {
    this.mapper = mapper;
  }
  @Override
  public void write(
      Object o, BufferOutput bufferOutput, Serializer serializer
  )
  {
    try {
      bufferOutput.writeString(mapper.writeValueAsString(o));
    } catch (IOException ex) {
      ex.printStackTrace();
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public Object read(
      Class aClass, BufferInput bufferInput, Serializer serializer
  )
  {
    try {
      return mapper.readValue(bufferInput.readString(), aClass);
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }
}
