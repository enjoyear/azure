package com.chen.guo;

import com.chen.guo.job.IntSumReducer;
import com.chen.guo.job.TokenizerMapper;
import com.microsoft.azure.storage.StorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HadoopWordCount2 {
  public static void main(String[] args) throws Exception {
    System.out.println("Running Word Count example...");
    HashMap<String, String> cosmos = new HashMap<>();
    cosmos.put("customer1", "6358f0cd-ce12-4e89-be99-66b16637880e");
    cosmos.put("customer2", "a75cef49-07f3-4028-bd1b-38731cf1ff4f");

    for (String arg : args) {
      System.out.println("arg: " + arg);
    }

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    /**
     * These three configurations will be added after applying the GenericOptionsParser
     *
     * Configuration content: mapreduce.job.tags -> job_1564700871853_0002
     * Configuration content: mapreduce.job.credentials.binary -> /mnt/resource/hadoop/yarn/local/usercache/admin/appcache/application_1564700871853_0002/container_1564700871853_0002_01_000002/container_tokens
     * Configuration content: mapreduce.client.genericoptionsparser.used -> true
     */
    //printConfiguration(conf);

    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }

    String clientNames = otherArgs[(otherArgs.length - 1)].toLowerCase();
    List<Job> jobs = new ArrayList<>();

    for (String clientName : clientNames.split(",")) {
      System.out.println("Starting job for " + clientName);
      Job job = createMRJob(cosmos, conf, otherArgs, clientName);
      jobs.add(job);
      //Submit the job to the cluster and return immediately.
      System.out.println(String.format("Job %s for client %s submitting", job.getJobID(), clientName));
      job.submit();
    }

    for (Job job : jobs) {
      if (!job.waitForCompletion(true)) {
        System.out.println(String.format("Job %s failed", job.getJobName()));
        System.exit(1);
      } else {
        System.out.println(String.format("Job %s completed", job.getJobName()));
      }
    }

    System.exit(0);
  }

  private static Job createMRJob(HashMap<String, String> cosmos, Configuration conf, String[] otherArgs, String clientName) throws URISyntaxException, InvalidKeyException, StorageException, IOException {
    String storageAccountConnectionString = otherArgs[(otherArgs.length - 2)];

    String clientId = cosmos.get(clientName);
    String clientSecret = CredentialsFileProvider.getSecretFromSA(storageAccountConnectionString, clientName + "-secret");
    System.out.println(String.format("Read AKV for client %s: got id %s, secret %s", clientName, clientId, clientSecret));
    conf.set("fs.azure.account.auth.type", "OAuth");
    conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
    conf.set("fs.azure.account.oauth2.client.id", clientId);
    conf.set("fs.azure.account.oauth2.client.secret", clientSecret);
    conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3/oauth2/token");

    //https://hadoop.apache.org/docs/r2.7.3/api/index.html?org/apache/hadoop/mapreduce/Job.html
    Job job = Job.getInstance(conf, "WC-" + clientName);
    job.setJarByClass(HadoopWordCount2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    String egPath = otherArgs[0];
    System.out.println("EG Path: " + egPath);
    FileInputFormat.addInputPath(job, new Path(egPath));

    for (int i = 1; i < otherArgs.length - 3; i++) {
      String inputPart = otherArgs[i];
      String inputPath = String.format("abfss://%s@%s", clientName, inputPart);
      System.out.println(String.format("Got input %s. Input path %s", inputPart, inputPath));
      FileInputFormat.addInputPath(job, new Path(inputPath));
    }

    FileSystem fs = FileSystem.get(conf);
    String outputPart = otherArgs[(otherArgs.length - 3)];
    String outputPath = String.format("abfss://%s@%s", clientName, outputPart);
    Path outputDir = new Path(outputPath, Long.toString(System.currentTimeMillis()));
    System.out.println(String.format("FS: %s. Output path: %s", fs.toString(), outputDir.toString()));

    FileOutputFormat.setOutputPath(job, outputDir);
    System.out.println(String.format("Finished configuration for job %s, id: %s", job.getJobName(), job.getJobID()));
    return job;
  }

  public static void printConfiguration(Configuration conf) {
    for (Map.Entry<String, String> next : conf) {
      System.out.println(String.format("Configuration content: %s -> %s", next.getKey(), next.getValue()));
    }
    System.out.println("****  Configuration content printed! ****");
  }
}