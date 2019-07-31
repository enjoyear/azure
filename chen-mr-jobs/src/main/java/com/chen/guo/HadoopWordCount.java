package com.chen.guo;

import com.chen.guo.security.KeyVaultADALAuthenticator;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.models.SecretBundle;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
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
import java.io.StringReader;
import java.util.StringTokenizer;

public class HadoopWordCount {
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

    Job job = Job.getInstance(conf, "Word Count Hadoop");
    job.setJarByClass(HadoopWordCount.class);
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

    String storageAccountConnectionString = otherArgs[(otherArgs.length - 1)];
    System.out.println(String.format("Storage Account Connection String: %s", storageAccountConnectionString));
    CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageAccountConnectionString);
    CloudBlobClient cloudBlobClient = storageAccount.createCloudBlobClient();
    CloudBlobContainer container = cloudBlobClient.getContainerReference("demo-jars");
    CloudBlockBlob blockRef = container.getBlockBlobReference("properties/azure_credentials.properties");
    String s = blockRef.downloadText();
    System.out.println("Blob Content: " + s);
    ICredentialProvider credentials = new CredentialsFileProvider(new StringReader(s));
    String sp = "akv-reader"; //this sp must be granted access in the KV's Access Policies
    String vaultURL = "https://chen-vault.vault.azure.net/";

    KeyVaultClient kvClient = new KeyVaultClient(KeyVaultADALAuthenticator.createCredentials(credentials, sp));
    SecretBundle secret = kvClient.getSecret(vaultURL, "sas-token");
    System.out.println("Fetched SAS token: " + secret.value());

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}