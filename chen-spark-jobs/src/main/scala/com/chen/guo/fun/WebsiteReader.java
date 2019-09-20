package com.chen.guo.fun;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

@Slf4j
public class WebsiteReader {
  public static void main(String[] args) throws IOException {
    readWebsite("https://www.foxnews.com/");
  }

  public static void readWebsite(String website) throws IOException {
    URL url = new URL(website);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    log.info("Original connection timeout: " + conn.getConnectTimeout());
    log.info("Original connection timeout: " + conn.getReadTimeout());
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);
    conn.setRequestMethod("GET");
    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String line;
    while ((line = rd.readLine()) != null) {
      log.info(line);
    }
    rd.close();
  }
}
