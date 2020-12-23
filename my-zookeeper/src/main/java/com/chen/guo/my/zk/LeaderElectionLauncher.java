package com.chen.guo.my.zk;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class LeaderElectionLauncher {
  public static final String ZK_CONNECTION_STRING = "localhost:2121";

  public static void main(String[] args)
      throws IOException {
    Scanner scanner = new Scanner(System.in);
    System.out.println("Enter service/machine name: ");
    String serviceMachineName = scanner.nextLine();
    System.out.println("Service/machine name is: " + serviceMachineName);

    //Shouldn't be cached thread as it will ignore spawned background thread.
    final ExecutorService service = Executors.newSingleThreadExecutor();

    final Future<?> submit = service.submit(new ProcessNode(serviceMachineName, ZK_CONNECTION_STRING));
  }
}
