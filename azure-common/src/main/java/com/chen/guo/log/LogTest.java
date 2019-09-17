package com.chen.guo.log;

import com.chen.guo.command.YarnCommandLineParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.MDC;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

@Slf4j
public class LogTest {
  public static void main(String[] args) throws ParseException, MalformedURLException, URISyntaxException {
    MDC.put("pipeline_runid", 123);
    MDC.put("activity_runid", 456);

    log.info("Hello");
    log.error("World");

    String command = "org.apache.spark.deploy.yarn.ApplicationMaster --class com.chen.guo.WordCountGen1MultipleSPs --jar wasbs://sparkjobs@abs0execution.blob.core.windows.net/3d4f7bc0-dd99-4f9e-8e41-3334244fa7af/17_09_2019_19_16_26_336/chen-spark-jobs-1.0-SNAPSHOT-all.jar --arg adl://adl0linkedin.azuredatalakestore.net/economic-graph/data/eg/linkedin-eg.txt --arg adl0enterprise.dfs.core.windows.net/data/gutenberg/davinci.txt --arg adl0conflated.dfs.core.windows.net/data/conflated/spark --arg DefaultEndpointsProtocol=https;AccountName=abs0jar;AccountKey=Um33mwAUmyxlOL1DN6RqyE/BrrXB1IVsp19JAnoyS9CO1dytPqtqKjE54cik+dOvPD8FfH9trjiBK7aDy1kohw==;EndpointSuffix=core.windows.net --arg 1237c048-eb65-4f07-abb2-02fde945eedb --properties-file /mnt/resource/hadoop/yarn/local/usercache/livy/appcache/application_1568744545360_0016/container_1568744545360_0016_04_000001/__spark_conf__/__spark_conf__.properties";
    log.info(YarnCommandLineParser.getActivityId(command));
  }
}
