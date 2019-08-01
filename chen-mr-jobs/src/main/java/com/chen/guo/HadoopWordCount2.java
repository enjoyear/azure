package com.chen.guo;

import com.microsoft.azure.storage.StorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class HadoopWordCount2 {
  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

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
    printConfiguration(conf);

    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }

    String clientNames = otherArgs[(otherArgs.length - 1)].toLowerCase();
    for (String clientName : clientNames.split(",")) {
      System.out.println("Starting job for " + clientName);
      Job job = wcJob(cosmos, conf, otherArgs, clientName);
      System.out.println(String.format("Job %s for client %s completed", job.getJobID(), clientName));

      if (!job.waitForCompletion(true)) {
        System.out.println(String.format("Job for client %s failed", clientName));
        System.exit(1);
      }
    }
    System.exit(0);
  }

  private static Job wcJob(HashMap<String, String> cosmos, Configuration conf, String[] otherArgs, String clientName) throws URISyntaxException, InvalidKeyException, StorageException, IOException {
    String storageAccountConnectionString = otherArgs[(otherArgs.length - 2)];

    String clientId = cosmos.get(clientName);
    String clientSecret = CredentialsFileProvider.getSecretFromSA(storageAccountConnectionString, clientName + "-secret");
    System.out.println(String.format("Got client id %s, client secret %s for client %s", clientId, clientSecret, clientName));
    conf.set("fs.azure.account.auth.type", "OAuth");
    conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
    conf.set("fs.azure.account.oauth2.client.id", clientId);
    conf.set("fs.azure.account.oauth2.client.secret", clientSecret);
    conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3/oauth2/token");

    Job job = Job.getInstance(conf, "Word Count Hadoop");
    job.setJarByClass(HadoopWordCount2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    for (int i = 0; i < otherArgs.length - 3; i++) {
      String inputPart = otherArgs[i];
      System.out.println("Got input " + inputPart);
      String inputPath = String.format("abfss://%s@%s", clientName, inputPart);
      FileInputFormat.addInputPath(job, new Path(inputPath));
    }

    FileSystem fs = FileSystem.get(conf);
    String outputPart = otherArgs[(otherArgs.length - 3)];
    String outputPath = String.format("abfss://%s@%s", clientName, outputPart);
    Path outputDir = new Path(outputPath, Long.toString(System.currentTimeMillis()));
    System.out.println(String.format("FS: %s. Output path: %s", fs.toString(), outputDir.toString()));

    FileOutputFormat.setOutputPath(job, outputDir);
    return job;
  }

  public static void printConfiguration(Configuration conf) {
    for (Map.Entry<String, String> next : conf) {
      System.out.println(String.format("Configuration content: %s -> %s", next.getKey(), next.getValue()));
    }
    System.out.println("****  Configuration content printed! ****");
  }
}