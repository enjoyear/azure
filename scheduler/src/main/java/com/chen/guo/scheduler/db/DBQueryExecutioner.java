package com.chen.guo.scheduler.db;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

@Slf4j
public class DBQueryExecutioner {
  private final QueryRunner _queryRunner;

  public DataSource getDataSource() {
    return _queryRunner.getDataSource();
  }

  /**
   * @param queryRunner a query runner that wraps a {@link BasicDataSource}.
   */
  public DBQueryExecutioner(QueryRunner queryRunner) {
    requireNonNull(queryRunner.getDataSource(), "data source must not be null.");
    _queryRunner = queryRunner;
  }

  /**
   * Execute a SELECT query through queryRunner.
   *
   * @param query         the query to execute
   * @param resultHandler the handler that creates the result object
   * @return The object returned by the handler.
   */
  public <T> T query(String query, ResultSetHandler<T> resultHandler, Object... params) throws SQLException {
    try {
      return _queryRunner.query(query, resultHandler, params);
    } catch (SQLException ex) {
      log.error(String.format("Select query failed: %s", query), ex);
      throw ex;
    }
  }

  /**
   * Executes an INSERT, UPDATE, or DELETE query through queryRunner.
   *
   * @param query  sql statements to execute
   * @param params initialize the PreparedStatement's IN parameters
   * @return The number of rows updated.
   */
  public int update(String query, Object... params) throws SQLException {
    try {
      return _queryRunner.update(query, params);
    } catch (SQLException ex) {
      log.error(String.format("Insert/Update/Delete query failed: %s", query), ex);
      throw ex;
    }
  }

  /**
   * Provide a way to allow users define custom SQL operations without relying on fixed SQL
   * interface. The common use case is to group a sequence of SQL operations without commit every
   * time.
   *
   * @param operations A sequence of DB operations
   * @param <T>        The type of object that the operations returns. Note that T could be null
   * @return T The object returned by the SQL statement, expected by the caller
   */
  public <T> T transaction(SQLTransaction<T> operations) throws SQLException {
    try (Connection conn = getDataSource().getConnection()) {
      conn.setAutoCommit(false);
      DBTransactionExecutioner transOperator = new DBTransactionExecutioner(_queryRunner, conn);
      T res = operations.execute(transOperator);
      conn.commit();
      return res;
    } catch (SQLException ex) {
      log.error("Transaction failed", ex);
      throw ex;
    }
  }
}
