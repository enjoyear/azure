package com.chen.guo.my.zk;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ProcessNode implements Runnable {
  public static final String NODEDATA_KEY_CURRENT_PROCESS = "CurrentProcess";
  public static final String NODEDATA_KEY_MONITORED_NODE_PATH = "MonitoredNodePath";
  public static final String NODEDATA_KEY_IS_LEADER = "IsLeader";
  public static final int DEFAULT_SESSION_TIMEOUT = 10000;
  private static final String LEADER_ELECTION_ROOT_NODE = "/leader-election";

  private final String processId; //the id for current process/service
  private final ZooKeeper zooKeeper;
  private final List<ACL> acl;
  //The path prefix for the ephemeral node created by ZK for current process/service
  private final String ephemeralNodePathPrefix;

  //the path to the ephemeral node created for current process/service
  private String ephemeralNodePath;

  /**
   * @param processId an ID for current service or process
   * @param zkConnString Comma separated host:port pairs with optional chroot suffix, each corresponding to a zk server.
   *                     e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
   * @throws IOException in cases of network failure while connecting to ZK
   */
  public ProcessNode(String processId, String zkConnString)
      throws IOException {
    this.processId = processId;
    this.zooKeeper = new ZooKeeper(zkConnString, DEFAULT_SESSION_TIMEOUT, null);
    log.info("Created zookeeper handle for " + zkConnString);
    this.acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    this.ephemeralNodePathPrefix = String.format("%s/srv_%s_", LEADER_ELECTION_ROOT_NODE, processId);
  }

  @Override
  public void run() {
    log.info(String.format("Process %s started...", processId));
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
          this.zooKeeper.create(this.ephemeralNodePathPrefix, emptyData, this.acl, CreateMode.EPHEMERAL_SEQUENTIAL);
      log.info(String.format("Ephemeral node %s created for the new joined process %s.", ephemeralNodePath, processId));

      attemptForLeader();
    } catch (Exception e) {
      log.error("Process initiation failed.", e);
    }
  }

  /**
   * Attempt for the leader position.
   * If current process' ephemeral id is the smallest among all processes, it's elected as the leader.
   */
  private void attemptForLeader()
      throws KeeperException, InterruptedException, JsonProcessingException {
    log.info(String.format("Process %s is attempting for a leader role.", this.processId));
    List<String> childrenNodeSeqNums = this.zooKeeper.getChildren(LEADER_ELECTION_ROOT_NODE, false);
    Collections.sort(childrenNodeSeqNums);
    log.info(
        String.format("Process %s sees ephemeral nodes: %s", this.processId, String.join(",", childrenNodeSeqNums)));
    //The ephemeral id assigned by ZK for current process/service
    final String procSequenceNum = this.ephemeralNodePath.substring(this.ephemeralNodePath.lastIndexOf('/') + 1);
    int index = childrenNodeSeqNums.indexOf(procSequenceNum);
    if (index == 0) {
      log.info(String.format("Process %s is elected as the new leader.", this.processId));
      electedAsLeader(procSequenceNum);
    } else {
      log.info(String.format("Process %s is NOT elected as the new leader.", this.processId));
      notElectedAsLeader(childrenNodeSeqNums, procSequenceNum);
    }
  }

  /**
   * Actions taken when current process/service is elected as the leader
   * @param procSequenceNum the ephemeral sequence number for current process
   */
  private void electedAsLeader(String procSequenceNum)
      throws JsonProcessingException, KeeperException, InterruptedException {
    Map<String, Object> data = new HashMap<>();
    data.put(NODEDATA_KEY_CURRENT_PROCESS, this.processId);
    data.put(NODEDATA_KEY_IS_LEADER, true);
    byte[] ephemeralData = JsonUtils.mapToJsonString(data).getBytes();

    String currentProcNodePath = LEADER_ELECTION_ROOT_NODE + "/" + procSequenceNum;
    this.zooKeeper.setData(currentProcNodePath, ephemeralData, -1);
  }

  /**
   * Actions taken when current process/service is NOT elected as the leader
   * @param childrenNodeSeqNums the ephemeral sequence numbers for all member processes
   *                            joining the leader election competition
   * @param procSequenceNum the ephemeral sequence number for current process
   */
  private void notElectedAsLeader(List<String> childrenNodeSeqNums, String procSequenceNum)
      throws KeeperException, InterruptedException, JsonProcessingException {
    int index = childrenNodeSeqNums.indexOf(procSequenceNum);
    String currentProcNodePath = LEADER_ELECTION_ROOT_NODE + "/" + procSequenceNum;

    //get the largest sequence number that is less than current procSequenceNum
    String watchedNodeSequenceNum = childrenNodeSeqNums.get(index - 1);
    String watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeSequenceNum;
    // Start watching the "previous" process/service to avoid Herd Effect
    // https://zookeeper.apache.org/doc/r3.5.5/recipes.html#sc_leaderElection
    log.info(String.format("Process %s will watch the node %s", this.processId, watchedNodePath));

    Stat nodeStat =
        this.zooKeeper.exists(watchedNodePath, new ProcessEphemeralNodeWatcher(currentProcNodePath, watchedNodePath));
    if (nodeStat != null) {
      log.info(String.format("Process %s started watching %s", this.processId, watchedNodePath));

      Map<String, Object> data = new HashMap<>();
      data.put(NODEDATA_KEY_CURRENT_PROCESS, this.processId);
      data.put(NODEDATA_KEY_MONITORED_NODE_PATH, watchedNodePath);
      data.put(NODEDATA_KEY_IS_LEADER, false);
      byte[] ephemeralData = JsonUtils.mapToJsonString(data).getBytes();
      this.zooKeeper.setData(currentProcNodePath, ephemeralData, -1);
    } else {
      //TODO: possibility should be extremely low
      throw new RuntimeException(String.format(
          "Process %s finds the ephemeral node %s lost between this.zooKeeper.getChildren and this.zooKeeper.exists",
          this.processId, watchedNodeSequenceNum));
    }
  }

  public class ProcessEphemeralNodeWatcher implements Watcher {

    private final String currentNodePath;
    private final String watchedNodePath;

    /**
     * @param currentNodePath the path to the ephemeral node created for current process
     * @param watchedNodePath the path to the ephemeral node under monitoring
     */
    public ProcessEphemeralNodeWatcher(String currentNodePath, String watchedNodePath) {
      this.currentNodePath = currentNodePath;
      this.watchedNodePath = watchedNodePath;
    }

    @Override
    public void process(WatchedEvent event) {
      final Event.EventType eventType = event.getType();
      log.info(
          String.format("Process %s(node %s) received event: %s", this.currentNodePath, processId, event.toString()));

      //If the watched service/node is lost, then current service/process should attempt for a leader role
      if (Event.EventType.NodeDeleted.equals(eventType)) {
        try {
          attemptForLeader();
        } catch (Exception e) {
          final String error = String
              .format("Process %s watcher failed while attempting for a leader role due to:%s", processId,
                  e.getMessage());
          log.error(error);
          throw new RuntimeException(error, e);
        }
      } else {
        log.info(String
            .format("Process %s will re-watch %s due to event %s", processId, this.watchedNodePath, event.toString()));
        try {
          zooKeeper.exists(this.watchedNodePath, this);
        } catch (Exception e) {
          final String error = String
              .format("Process %s watcher failed while trying to re-watch %s due to:%s", processId,
                  this.watchedNodePath, e.getMessage());
          log.error(error);
          throw new RuntimeException(error, e);
        }
      }
    }
  }
}