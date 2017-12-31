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

import com.metamx.common.logger.Logger;

/**
 */
public class CopycatSetupTest
{

  private static final Logger log = new Logger(CopycatSetupTest.class);
/*
  @Test(timeout = 300000)
  public void testBootstrapServersChangeSimple() throws Exception
  {
    String server1 = "localhost:9001";
    String server2 = "localhost:9002";
    List<String> bootstrapServers = ImmutableList.of(server1);

    Node node1 = new Node(server1, "localhost", 9001, ImmutableSet.of(new CoordinatorRole()));
    Node node2 = new Node(server2, "localhost", 9002, ImmutableSet.of(new CoordinatorRole()));

    CountDownLatch latch1 = new CountDownLatch(1);
    Thread serverThread1 = createNoGroupJoinerThread(new AtomixConfig(server1, bootstrapServers), node1, latch1, false);
    serverThread1.start();
    latch1.await();

//    Thread.sleep(5000);

    AtomixConfig config = new AtomixConfig(null, ImmutableList.of(server1));
    CopycatServerProvider server = new CopycatServerProvider(null, null, true);
    CopycatClientProvider client = new CopycatClientProvider("grouper", server, config, TestHelper.getObjectMapper());
    client.start();
    GroupStateMachineClient stateMachineClient = new GroupStateMachineClient(
        node1,
        client
    );
    stateMachineClient.start();

    TestListener listener = new TestListener();
    stateMachineClient.registerGroupChangeListener(NodeDiscovery.COORDINATOR_ID, listener);

    while(client.thisState != CopycatClientProvider.State.CONNECTED) {
      System.out.println("0::" + client.thisState);
      Thread.sleep(400);
    }

    //change bootstarp server list
    config.bootstrapServers = ImmutableList.of(server2);

    //stop server1
    serverThread1.interrupt();
//    serverThread1.join();

    while(client.thisState != CopycatClientProvider.State.SUSPENDED) {
      System.out.println("1::" + client.thisState);
      Thread.sleep(400);
    }

    CountDownLatch latch2 = new CountDownLatch(1);
    Thread serverThread2 = createNoGroupJoinerThread(
        new AtomixConfig(server2, ImmutableList.of(server2)),
        node2,
        latch2,
        false
    );
    serverThread2.start();

    while(client.thisState != CopycatClientProvider.State.CONNECTED) {
      System.out.println("2::" + client.thisState);
      Thread.sleep(400);
    }
  }

  @Test(timeout = 300000)
  public void testBootstrapServersChange() throws Exception
  {
    String server1 = "localhost:9001";
    String server2 = "localhost:9002";
    List<String> bootstrapServers = ImmutableList.of(server1);

    Node node1 = new Node(server1, "localhost", 9001, ImmutableSet.of(new CoordinatorRole()));
    Node node2 = new Node(server2, "localhost", 9002, ImmutableSet.of(new CoordinatorRole()));

    CountDownLatch latch1 = new CountDownLatch(1);
    Thread serverThread1 = createThread(new AtomixConfig(server1, bootstrapServers), node1, latch1, false);
    serverThread1.start();
    latch1.await();

//    Thread.sleep(5000);

    AtomixConfig config = new AtomixConfig(null, ImmutableList.of(server1));
    CopycatServerProvider server = new CopycatServerProvider(null, null, true);
    CopycatClientProvider client = new CopycatClientProvider("grouper", server, config, TestHelper.getObjectMapper());
    client.start();
    GroupStateMachineClient stateMachineClient = new GroupStateMachineClient(
        node1,
        client
    );
    stateMachineClient.start();

    TestListener listener = new TestListener();
    stateMachineClient.registerGroupChangeListener(NodeDiscovery.COORDINATOR_ID, listener);

    while(!ImmutableSet.of(node1).equals(listener.nodes)) {
      System.out.println("0::" + listener.nodes);
      Thread.sleep(400);
    }

    //change bootstarp server list
    config.bootstrapServers = ImmutableList.of(server2);

    //stop server1
    serverThread1.interrupt();
//    serverThread1.join();

    CountDownLatch latch2 = new CountDownLatch(1);
    Thread serverThread2 = createThread(new AtomixConfig(server2, ImmutableList.of(server2)),
                                        node2,
                                        latch2,
                                        false
    );
    serverThread2.start();

    while(!ImmutableSet.of(node2).equals(listener.nodes)) {
      System.out.println("1::" + listener.nodes);
      Thread.sleep(400);
    }
  }

  private Thread createNoGroupJoinerThread(final AtomixConfig config, final Node node, final CountDownLatch latch, final boolean noopServer)
  {
    Runnable runnable = new Runnable()
    {
      @Override
      public void run()
      {
        CopycatServerProvider server = new CopycatServerProvider(config, TestHelper.getObjectMapper(), false);
//        CopycatClientProvider client = new CopycatClientProvider(node.getName(), server, config, TestHelper.getObjectMapper());
//        GroupJoiner groupJoiner = new GroupJoiner(client, node, TestHelper.getObjectMapper());

        server.start();
//        client.start();
//        groupJoiner.start();

        latch.countDown();

        try {
          Thread.sleep(365*24*60*60*1000); //sleep forever
        } catch (InterruptedException ex) {
//          groupJoiner.stop();
//          client.stop();
          server.stop();
        }
      }
    };

    return new Thread(runnable);
  }
*/
  //TODO: this does not test when multiple servers go down simultaneously
/*  @Test(timeout = 60000)
  public void testIt() throws Exception
  {
    registerEmitter();

    String server1 = "localhost:9001";
    String server2 = "localhost:9002";
    String server3 = "localhost:9003";
    List<String> bootstrapServers = ImmutableList.of(server1, server2, server3); //TODO: a server4 should just be able to join

    Node node1 = new Node(server1, "localhost", 9001, ImmutableSet.of(new CoordinatorRole()));
    Node node2 = new Node(server2, "localhost", 9002, ImmutableSet.of(new CoordinatorRole()));
    Node node3 = new Node(server3, "localhost", 9003, ImmutableSet.of(new CoordinatorRole()));

    CountDownLatch latch1 = new CountDownLatch(1);
    Thread serverThread1 = createThread(new AtomixConfig(server1, bootstrapServers), node1, latch1, false);
    serverThread1.start();

    CountDownLatch latch2 = new CountDownLatch(1);
    Thread serverThread2 = createThread(new AtomixConfig(server2,  bootstrapServers), node2, latch2, false);
    serverThread2.start();

    AtomixConfig config = new AtomixConfig(null, ImmutableList.of(server1));
    CopycatServerProvider server = new CopycatServerProvider(null, null, true);
    CopycatClientProvider client = new CopycatClientProvider("grouper", server, config, TestHelper.getObjectMapper());
    client.start();
    GroupStateMachineClient stateMachineClient = new GroupStateMachineClient(
        client
    );
    stateMachineClient.start();

    CopycatLeaderDiscovery leaderDiscovery = new CopycatLeaderDiscovery(node1, client);
    leaderDiscovery.start();

    TestListener listener = new TestListener();
    stateMachineClient.registerGroupChangeListener(listener);

    TestLeadershipListener leadershipListener = new TestLeadershipListener();
    leaderDiscovery.registerCoordinatorLeadershipListener(leadershipListener);

    while(!ImmutableSet.of(node1, node2).equals(listener.nodes)) {
      Thread.sleep(400);
    }

    CountDownLatch latch3 = new CountDownLatch(3);
    Thread serverThread3 = createThread(new AtomixConfig(server3,  bootstrapServers), node3, latch3, false);
    serverThread3.start();

    while(!ImmutableSet.of(node1, node2, node3).equals(listener.nodes)) {
      System.out.println("0::" + listener.nodes);
      Thread.sleep(400);
    }

    //stop server2
    serverThread2.interrupt();
    serverThread2.join();

    while(!ImmutableSet.of(node1, node3).equals(listener.nodes)) {
      System.out.println("1::" + listener.nodes);
      Thread.sleep(400);
    }

    //restart server2
    latch2 = new CountDownLatch(1);
    serverThread2 = createThread(new AtomixConfig(server2,  bootstrapServers), node2, latch2, false);
    serverThread2.start();

    while(!ImmutableSet.of(node1, node2, node3).equals(listener.nodes)) {
      System.out.println("2::" + listener.nodes);
      Thread.sleep(400);
    }

    //stop server1
    serverThread1.interrupt();
    serverThread1.join();

    //TODO: check leader coordinator to ensure that it has been disposed of

    //note that even in group client only knew about server1 to begin with but, it discovered other servers
    //and continues to work even if server1 went down
    while(!ImmutableSet.of(node2, node3).equals(listener.nodes)) {
      System.out.println("3::" + listener.nodes);
      Thread.sleep(100);
    }

    serverThread2.interrupt();
    serverThread2.join();

    //with server2 down, we lost the quorum and no updates are made but server2 removed node2 before closing
    //copycat server
    while(!ImmutableSet.of(node3).equals(listener.nodes)) {
      System.out.println("4::" + listener.nodes);
      Thread.sleep(100);
    }

    //restart server1 to get the quorum
    latch1 = new CountDownLatch(1);
    serverThread1 = createThread(new AtomixConfig(server1, bootstrapServers), node1, latch1, false);
    serverThread1.start();
    latch1.await();

    while(!ImmutableSet.of(node1, node3).equals(listener.nodes)) {
      System.out.println("5::" + listener.nodes);
      Thread.sleep(100);
    }

    //test recovery of CopycatClientProvider
    server1 = "localhost:9011";
    server2 = "localhost:9012";
    server3 = "localhost:9013";
    bootstrapServers = ImmutableList.of(server1, server2, server3); //TODO: a server4 should just be able to join
    config.bootstrapServers = ImmutableList.of(server1);

    serverThread1.interrupt();
    serverThread2.interrupt();
    serverThread3.interrupt();
    serverThread1.join();
    serverThread2.join();
    serverThread3.join();

    node1 = new Node(server1, "localhost", 9011, ImmutableSet.of(new CoordinatorRole()));
    node2 = new Node(server2, "localhost", 9012, ImmutableSet.of(new CoordinatorRole()));
    node3 = new Node(server3, "localhost", 9013, ImmutableSet.of(new CoordinatorRole()));

    latch1 = new CountDownLatch(1);
    serverThread1 = createThread(new AtomixConfig(server1, bootstrapServers), node1, latch1, false);
    serverThread1.start();

    latch2 = new CountDownLatch(1);
    serverThread2 = createThread(new AtomixConfig(server2,  bootstrapServers), node2, latch2, false);
    serverThread2.start();



    while(!ImmutableSet.of(node1, node2).equals(listener.nodes)) {
      System.out.println("6::" + listener.nodes);
      Thread.sleep(400);
    }

    latch3 = new CountDownLatch(1);
    serverThread3 = createThread(new AtomixConfig(server3,  bootstrapServers), node3, latch3, false);
    serverThread3.start();

    while(!ImmutableSet.of(node1, node2, node3).equals(listener.nodes)) {
      System.out.println("7::" + listener.nodes);
      Thread.sleep(400);
    }

    //TODO: check leader again

    //shutdown two servers and lose the quorum
//    serverThread1.interrupt();
//    serverThread2.interrupt();
//    serverThread3.interrupt();
//    serverThread1.join();
//    serverThread2.join();
//    serverThread3.join();

//    serverThread2.interrupt();
//    serverThread3.interrupt();
//    serverThread2.join();
//    serverThread3.join();

//    while(!ImmutableSet.of(node1, node2, node3).equals(listener.nodes)) {
//      Thread.sleep(100);

    //TODO: test if all servers went down and new servers came back up, then GroupStateMachineClient continues to work.

//    }
  }

  private Thread createThread(final AtomixConfig config, final Node node, final CountDownLatch latch, final boolean noopServer)
  {
    Runnable runnable = new Runnable()
    {
      @Override
      public void run()
      {
        CopycatServerProvider server = new CopycatServerProvider(config, TestHelper.getObjectMapper(), false);
        CopycatClientProvider client = new CopycatClientProvider(node.getName(), server, config, TestHelper.getObjectMapper());
        GroupJoiner groupJoiner = new GroupJoiner(client, node, TestHelper.getObjectMapper());

        client.start();
        groupJoiner.start();

        latch.countDown();

        try {
          Thread.sleep(365*24*60*60*1000); //sleep forever
        } catch (InterruptedException ex) {
          groupJoiner.stop();
          client.stop();
        }
      }
    };

    return new Thread(runnable);
  }

  private void registerEmitter()
  {
    ServiceEmitter emitter = new ServiceEmitter(
        "service",
        "host",
        new LoggingEmitter(
            log,
            LoggingEmitter.Level.ERROR,
            TestHelper.getObjectMapper()
        )
    );
    EmittingLogger.registerEmitter(emitter);
    emitter.start();
  }
  private static class TestListener implements NodeGroupChangeListener
  {
    Set<Node> nodes = new ConcurrentHashSet<>();

    @Override
    public void initialized()
    {
      log.info("I'm initialized.");
    }

    @Override
    public void serverAdded(Node node)
    {
      if (nodes.contains(node)) {
        throw new RE("Node [%s] already exists.", node);
      } else {
        nodes.add(node);
      }
    }

    @Override
    public void serverRemoved(Node node)
    {
      if (nodes.contains(node)) {
        nodes.remove(node);
      } else {
        throw new RE("Node [%s] does not exist.", node);
      }
    }
  }

  private static class TestLeadershipListener implements LeaderDiscovery.Listener
  {
    static String TAKE = "take";
    static String LEAVE = "leave";

    List<String> changes = new ArrayList<>();

    @Override
    public void takeLeadership()
    {
      changes.add(TAKE);
      System.err.println("takeLeadership");
    }

    @Override
    public void leaveLeadership()
    {
      changes.add(LEAVE);
      System.err.println("leaveLeadership");
    }
  } */
}

