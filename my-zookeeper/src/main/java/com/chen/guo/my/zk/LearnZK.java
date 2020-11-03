package com.chen.guo.my.zk;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;


public class LearnZK implements Watcher {
  static ZooKeeper zoo;
  public final static String connectionString = "localhost:2121";
  public final static int defaultSessionTimeout = 10000;

  public static void main(String[] args)
      throws Exception {

    final String path = "/tutorial";
    byte[] data = "We can learn most of the technologies here".getBytes();

    zoo = new ZooKeeper(connectionString, defaultSessionTimeout, new LearnZK());
    if (zoo.exists(path, false) == null) {
      zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    final Watcher getChildrenWatch = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        System.out.println("getChildren notified...   " + event.getPath() + ":" + event.getType());
        try {
          zoo.getData(path, this, null);
        } catch (Exception e) {

        }
      }
    };

    final Watcher getDataWatch = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (true) {
          throw new RuntimeException("Abc");
        }

        System.out.println("getData notified...   " + event.getPath() + ":" + event.getType());
        try {
          zoo.getData(path, this, null);
        } catch (Exception e) {

        }
      }
    };

    final List<String> children = zoo.getChildren(path, true);
    System.out.println(children);

    final byte[] fetched = zoo.getData(path, getDataWatch, null);

    zoo.exists(path, getDataWatch);

    Thread.sleep(1000000);
  }

  private void electMaster(String id) {
    boolean isLeader = false;
    final String path = String.format("/leader-election/id%s_", id);

    try {
      ZooKeeper zkHandle = new ZooKeeper(connectionString, defaultSessionTimeout, this);

      while (true) {
        try {
          //Tries to create the master lock
          final String node = zkHandle.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
          System.out.println("Node created: " + node);
          isLeader = true;
        } catch (KeeperException.NodeExistsException e) {
          isLeader = false;
        }

        final CountDownLatch latch = new CountDownLatch(1);
        zkHandle.exists(path, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            //Watch the master lock
            latch.countDown();
          }
        });

        if (isLeader) {
          System.out.println("Elected as the leader");
        } else {
          System.out.println("NOT elected as the leader");
        }

        //Blocks while there is a master
        latch.await();
      }
    } catch (Exception e) {

    }
  }

  @Override
  public void process(WatchedEvent event) {

    System.out
        .println(String.format("From default watcher: path(%s), %s", event.getPath(), event.getType().toString()));
  }
}
