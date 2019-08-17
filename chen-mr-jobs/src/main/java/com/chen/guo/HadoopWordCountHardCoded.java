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

public class HadoopWordCountHardCoded {
  public static void main(String[] args) throws Exception {
    System.out.println("Running Word Count example...");
    for (String arg : args) {
      System.out.println("arg: " + arg);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word Count Hadoop");
    job.setJarByClass(HadoopWordCountHardCoded.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    Path input1 = new Path("abfss://economic-graph@adl0linkedin.dfs.core.windows.net/data/eg/linkedin-eg.txt");
    System.out.println(String.format("Input1: %s", input1.toString()));
    Path input2 = new Path("abfss://customer1@adl0enterprise.dfs.core.windows.net/data/gutenberg/davinci.txt");
    System.out.println(String.format("Input2: %s", input2.toString()));
    FileInputFormat.addInputPath(job, input1);
    FileInputFormat.addInputPath(job, input2);

    FileSystem fs = FileSystem.get(conf);
    Path outputDir = new Path("abfss://customer1@adl0conflated.dfs.core.windows.net/data/conflated/mr", Long.toString(System.currentTimeMillis()));

    System.out.println(String.format("FS: %s. Output path: %s", fs.toString(), outputDir.toString()));
    FileOutputFormat.setOutputPath(job, outputDir);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}