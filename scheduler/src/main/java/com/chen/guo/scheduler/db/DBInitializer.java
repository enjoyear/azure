package com.chen.guo.scheduler.db;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * A Util class that sets up database schemas based on sql script files
 */
@Slf4j
public class DBInitializer {
  private static final String SQL_SETUP_PREFIX = "setup.";
  private static final String SQL_SETUP_SUFFIX = ".sql";

  private final BasicDataSource _dataSource;
  private final String _setupDir;

  /**
   * @param ds       the data source
   * @param setupDir a directory containing all SQL exec scripts.
   *                 The scripts should follow the pattern exec.<table-name>.sql
   */
  public DBInitializer(BasicDataSource ds, String setupDir) {
    _dataSource = ds;
    _setupDir = setupDir;
  }

  /**
   * Initialize the exec of a database
   */
  public void init() throws SQLException, IOException {
    Set<String> setups = findAllSetups();
    exec(setups);
  }

  /**
   * @return all MySQL exec files following the pattern {@link #SQL_SETUP_PREFIX}*{@link #SQL_SETUP_SUFFIX}
   */
  private Set<String> findAllSetups() {
    Set<String> tables = new HashSet<>();
    File directory = new File(_setupDir);
    File[] createScripts = directory.listFiles(pathname -> {
      if (!pathname.isFile() || pathname.isHidden()) {
        return false;
      }

      String name = pathname.getName();
      return name.startsWith(SQL_SETUP_PREFIX) && name.endsWith(SQL_SETUP_SUFFIX);
    });

    if (createScripts != null) {
      for (File script : createScripts) {
        String name = script.getName();
        String[] nameSplit = name.split("\\.");
        String tableName = nameSplit[1];
        tables.add(tableName);
      }
    }
    return tables;
  }

  /**
   * Execute all set up sql scripts
   *
   * @param setups all sql script setups
   * @throws SQLException if there is an issue executing setup sql
   * @throws IOException  if the setup sql files cannot be loaded properly
   */
  private void exec(final Set<String> setups) throws SQLException, IOException {
    final Connection conn = _dataSource.getConnection();
    conn.setAutoCommit(false);
    try {
      for (String setup : setups) {
        executeSqlScript(conn, setup);
      }
    } finally {
      conn.close();
    }
  }

  private void executeSqlScript(Connection conn, String setup)
      throws IOException, SQLException {
    String fileName = SQL_SETUP_PREFIX + setup + SQL_SETUP_SUFFIX;
    File script = new File(_setupDir, fileName);
    Path setupFilePath = script.toPath();
    log.info(String.format("Executing new setup %s...", setupFilePath.toString()));

    String content = new String(Files.readAllBytes(setupFilePath));
    String[] queries = content.split(";\\s*\n");
    QueryRunner runner = new QueryRunner();
    for (String query : queries) {
      runner.update(conn, query);
    }
    conn.commit();
  }
}
