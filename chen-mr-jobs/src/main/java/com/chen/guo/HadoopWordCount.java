package com.chen.guo;

import com.chen.guo.job.IntSumReducer;
import com.chen.guo.job.TokenizerMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopWordCount {
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

    Job job = Job.getInstance(conf, "Word Count Hadoop");
    job.setJarByClass(HadoopWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    for (int i = 0; i < otherArgs.length - 1; i++) {
      Path path = new Path(otherArgs[i]);
      System.out.println(String.format("Input path: %s", path.toString()));
      FileInputFormat.addInputPath(job, path);
    }

    FileSystem fs = FileSystem.get(conf);
    Path outputDir = new Path(otherArgs[(otherArgs.length - 1)], Long.toString(System.currentTimeMillis()));
    System.out.println(String.format("Output path: %s. FS: %s. ", outputDir.toString(), fs.toString()));
    FileOutputFormat.setOutputPath(job, outputDir);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}