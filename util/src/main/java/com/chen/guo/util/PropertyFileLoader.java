package com.chen.guo.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileLoader {
  public static Properties load(String path) {
    try (InputStream input = new FileInputStream(path)) {
      Properties prop = new Properties();
      // load a properties file
      prop.load(input);
      return prop;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
