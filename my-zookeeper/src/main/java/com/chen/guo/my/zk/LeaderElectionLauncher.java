package com.chen.guo.my.zk;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class LeaderElectionLauncher {
  public static final String ZK_CONNECTION_STRING = "localhost:2121";

  public static void main(String[] args)
      throws IOException {

    final ExecutorService service = Executors.newSingleThreadExecutor();

    final Future<?> status = service.submit(new ProcessNode("123", ZK_CONNECTION_STRING));

    try {
      status.get();
    } catch (InterruptedException | ExecutionException e) {
      log.error(e.getMessage(), e);
      service.shutdown();
    }
  }
}
