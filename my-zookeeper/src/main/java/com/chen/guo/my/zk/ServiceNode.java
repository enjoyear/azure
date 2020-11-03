package com.chen.guo.my.zk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ServiceNode implements Runnable {
  public static final String ZK_CONNECTION_STRING = "localhost:2121";
  public static final int DEFAULT_SESSION_TIMEOUT = 10000;
  private static final String LEADER_ELECTION_ROOT_NODE = "/leader-election";
  //The path prefix for the ephemeral node created by ZK for current service
  private static final String EPHEMERAL_NODE_PATH_PREFIX = LEADER_ELECTION_ROOT_NODE + "/m_";

  private final String serviceId; //the id for current service
  private final ZooKeeper zooKeeper;
  private final List<ACL> acl;

  //the path to the ephemeral node created for current service
  private String ephemeralNodePath;

  public static void main(String[] args)
      throws IOException {
    final ServiceNode id123 = new ServiceNode("123", ZK_CONNECTION_STRING);
    id123.run();
  }

  /**
   * @param serviceId an ID for current service or process
   * @param zkConnString Comma separated host:port pairs with optional chroot suffix, each corresponding to a zk server.
   *                     e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
   * @throws IOException in cases of network failure while connecting to ZK
   */
  public ServiceNode(String serviceId, String zkConnString)
      throws IOException {
    this.serviceId = serviceId;
    this.zooKeeper = new ZooKeeper(zkConnString, DEFAULT_SESSION_TIMEOUT, null);
    log.info("Created zookeeper handle for " + zkConnString);
    this.acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  }

  @Override
  public void run() {
    log.info(String.format("Service %s started...", serviceId));
    final byte[] emptyData = new byte[0];

    try {
      //Create the permanent ROOT node if not exist
      Stat nodeStat = this.zooKeeper.exists(LEADER_ELECTION_ROOT_NODE, false);
      if (nodeStat == null) {
        log.info("Leader election root node doesn't exist. Creating node " + LEADER_ELECTION_ROOT_NODE);
        this.zooKeeper.create(LEADER_ELECTION_ROOT_NODE, emptyData, this.acl, CreateMode.PERSISTENT);
        log.info("Leader election root node created.");
      }

      //Create the ephemeral member node
      this.ephemeralNodePath =
          this.zooKeeper.create(EPHEMERAL_NODE_PATH_PREFIX, emptyData, this.acl, CreateMode.EPHEMERAL_SEQUENTIAL);
      log.info(String
          .format("Member node %s created for the new joined service %s.", EPHEMERAL_NODE_PATH_PREFIX, serviceId));

      attemptForLeader();
    } catch (Exception e) {
      log.error("Service initiation failed.", e);
    }
  }

  /**
   * Attempt for the leader position.
   * If current service' member id is the smallest among all members, it's elected as the leader.
   */
  private void attemptForLeader()
      throws KeeperException, InterruptedException {
    List<String> childNodeSequenceNums = this.zooKeeper.getChildren(LEADER_ELECTION_ROOT_NODE, false);
    Collections.sort(childNodeSequenceNums);

    //The ephemeral id assigned by ZK for current service
    final String serviceSequenceNum = this.ephemeralNodePath.substring(this.ephemeralNodePath.lastIndexOf('/') + 1);

    int index = childNodeSequenceNums.indexOf(serviceSequenceNum);
    if (index == 0) {
      leaderAssigned();
    } else {
      //get the largest sequence number that is less than current serviceSequenceNum
      String watchedNodeSequenceNum = childNodeSequenceNums.get(index - 1);
      String watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeSequenceNum;
      // Start watching the "previous" service to avoid Herd Effect
      // https://zookeeper.apache.org/doc/r3.5.5/recipes.html#sc_leaderElection
      log.info(String.format("Service %s: will watch the node %s", this.serviceId, watchedNodePath));

      Stat nodeStat = this.zooKeeper.exists(watchedNodePath, new ServiceNodeWatcher());
      if (nodeStat != null) {
        log.info(String.format("Service %s: started watching %s", this.serviceId, watchedNodePath));
      } else {
        //TODO: possibility should be extremely low
        throw new RuntimeException(String.format(
            "The service for the ephemeral node %s is lost between this.zooKeeper.getChildren and this.zooKeeper.exists",
            watchedNodeSequenceNum));
      }
    }
  }

  /**
   * Actions taken when current service is elected as the leader
   */
  private void leaderAssigned() {
    log.info(String.format("Service %s is elected as the new leader.", this.serviceId));
  }

  public class ServiceNodeWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      final Event.EventType eventType = event.getType();
      log.debug(String.format("Service %s: event '%s' received", serviceId, eventType));

      //If the watched service/node is lost, then current service/process should attempt for a leader role
      if (Event.EventType.NodeDeleted.equals(eventType)) {
        try {
          attemptForLeader();
        } catch (Exception e) {
          log.error("Watcher failed while attempting for a leader role due to:" + e.getMessage());
          throw new RuntimeException(e);
        }
      }
    }
  }
}