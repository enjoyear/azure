package com.chen.guo;

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
    for (String arg : args) {
      System.out.println("arg: " + arg);
    }

    Configuration conf = new Configuration();

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }

    conf.set("fs.azure.account.auth.type", "OAuth");
    conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");

    System.out.println("Got client id: " + otherArgs[(otherArgs.length - 1)]);
    conf.set("fs.azure.account.oauth2.client.id", otherArgs[(otherArgs.length - 1)]);
    conf.set("fs.azure.account.oauth2.client.secret", "8pTfD[X6bDS1eVtWi8nUKvuCj+=/9k=X");
    conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3/oauth2/token");

    Job job = Job.getInstance(conf, "Word Count Hadoop");
    job.setJarByClass(HadoopWordCount2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    for (int i = 0; i < otherArgs.length - 2; i++) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    FileSystem fs = FileSystem.get(conf);
    Path outputDir = new Path(otherArgs[(otherArgs.length - 2)], Long.toString(System.currentTimeMillis()));
    System.out.println(String.format("FS: %s. Output path: %s", fs.toString(), outputDir.toString()));

    FileOutputFormat.setOutputPath(job, outputDir);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}