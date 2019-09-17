package com.chen.guo.command;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class YarnCommandLineParser {
  public static String getActivityId(String command) throws ParseException, MalformedURLException, URISyntaxException {
    String[] splits = command.split(" ");
    Options options = new Options();
    options.addOption("c", "class", true, "main class name");
    options.addOption("j", "jar", true, "main jar path");
    options.addOption("a", "arg", true, "main jar path");
    options.addOption("p", "properties-file", true, "main jar path");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, splits);
    //wasbs://sparkjobs@abs0execution.blob.core.windows.net/d4bdae3c-6394-4eac-aa56-c82581a6b803/17_09_2019_18_46_25_014/chen-spark-jobs-1.0-SNAPSHOT-all.jar

    String jarPath = cmd.getOptionValue("jar");
    URI uri = new URI(jarPath);
    log.info("protocol or schema = " + uri.getScheme());
    log.info("authority = " + uri.getAuthority());
    log.info("host = " + uri.getHost());
    log.info("port = " + uri.getPort());
    log.info("path = " + uri.getPath());
    log.info("query = " + uri.getQuery());
    //System.out.println("filename = " + uri.getFile());
    //System.out.println("ref = " + uri.getRef());
    Path path = Paths.get(uri.getPath());
    return path.getName(0).toString(); //return the first folder name, which is the activity run id
  }
}
