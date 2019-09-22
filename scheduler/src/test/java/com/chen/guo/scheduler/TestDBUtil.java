package com.chen.guo.scheduler;

import com.chen.guo.scheduler.db.DBInitializer;
import com.chen.guo.scheduler.db.DBQueryExecutioner;
import com.chen.guo.scheduler.db.MySQLDataSource;
import com.chen.guo.util.PropertyFileLoader;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class TestDBUtil {

  private final String _testDbPropFile;
  private final String _dbSetupDir;
  private DBQueryExecutioner _dbQueryExecutioner;

  public TestDBUtil() {
    this("src/test/resources/db-connection.properties", "src/test/resources");
  }

  /**
   * @param dbConnectionProp the relative path to the db connection property file
   * @param dbSetupDir       the relative directory that contains db setup sql files
   */
  public TestDBUtil(String dbConnectionProp, String dbSetupDir) {
    _testDbPropFile = dbConnectionProp;
    _dbSetupDir = dbSetupDir;
    init();
  }

  /**
   * initialize the database
   */
  public void init() {
    Properties properties = PropertyFileLoader.load(_testDbPropFile);
    BasicDataSource dataSource = new MySQLTestDataSource(properties);

    //Create the database if not exist with a connection to the database server
    String dbName = ((MySQLDataSource) dataSource).getDbName();
    DBQueryExecutioner dbCreator = new DBQueryExecutioner(new QueryRunner(((MySQLTestDataSource) dataSource).getServerDataSource()));
    try {
      dbCreator.update("CREATE DATABASE IF NOT EXISTS " + dbName);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    //Initialize all tables with a connection to a database schema
    DBInitializer setup = new DBInitializer(dataSource, new File(_dbSetupDir).getAbsolutePath());
    try {
      setup.init();
    } catch (SQLException | IOException e) {
      throw new RuntimeException(e);
    }

    //Create the queryExecutioner with a connection to a database schema
    _dbQueryExecutioner = new DBQueryExecutioner(new QueryRunner(dataSource));
  }

  public DBQueryExecutioner getDBQueryExecutioner() {
    if (_dbQueryExecutioner != null) {
      return _dbQueryExecutioner;
    } else {
      init();
      return _dbQueryExecutioner;
    }
  }

}
