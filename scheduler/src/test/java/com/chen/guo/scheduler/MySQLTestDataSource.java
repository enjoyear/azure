package com.chen.guo.scheduler;

import com.chen.guo.scheduler.db.MySQLDataSource;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
@Getter
@Accessors(prefix = {"_"})
public class MySQLTestDataSource extends MySQLDataSource {

  public MySQLTestDataSource(Properties properties) {
    super(properties);
  }

  /**
   * Return a data source that connects to a database server
   */
  public MySQLDataSource getServerDataSource() {
    String hostname = _properties.getProperty("mysql.hostname");
    String port = _properties.getProperty("mysql.port");
    MySQLDataSource mySQLDataSource = new MySQLDataSource(_properties);
    mySQLDataSource.setUrl(String.format("jdbc:mysql://%s:%s", hostname, port));
    return mySQLDataSource;
  }
}
