package com.chen.guo.my.zk;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class LeaderElectionLauncher {
  public static final String ZK_CONNECTION_STRING = "localhost:2121";

  public static void main(String[] args)
      throws IOException {

    final ExecutorService service = Executors.newSingleThreadExecutor();

    for (int i = 0; i < 5; ++i) {
      service.submit(new ProcessNode("task" + i, ZK_CONNECTION_STRING));
    }
  }
}
