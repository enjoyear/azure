package com.chen.guo.scheduler.db;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;

import java.util.Properties;

@Slf4j
@Getter
@Accessors(prefix = {"_"})
public class MySQLDataSource extends BasicDataSource {
  protected final Properties _properties;
  private final String _dbName;

  public MySQLDataSource(Properties properties) {
    super();
    _properties = properties;

    _dbName = properties.getProperty("mysql.db");
    setDriverClassName(properties.getProperty("mysql.driver"));
    setUsername(properties.getProperty("mysql.user"));
    setPassword(properties.getProperty("mysql.password"));

    String hostname = properties.getProperty("mysql.hostname");
    String port = properties.getProperty("mysql.port");

    String urlWithDB = String.format("jdbc:mysql://%s:%s/%s", hostname, port, _dbName);
    setUrl(urlWithDB);
    setMaxTotal(Integer.valueOf(properties.getProperty("mysql.connection.max")));
  }
}
