package com.chen.guo.scheduler.db;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.Connection;
import java.sql.SQLException;


/**
 * This interface is designed as an supplement of {@link DBQueryExecutioner}, which do commit at the
 * end of every query. Given this interface, users/callers (implementation code) should decide where
 * to {@link Connection#commit()} based on their requirements.
 * <p>
 * The diff between DBTransactionExecutioner and DBQueryExecutioner: * Auto commit and Auto close
 * connection are enforced in DBQueryExecutioner, but not enabled in DBTransactionExecutioner. * We
 * usually group a couple of sql operations which need the same connection into
 * DBTransactionExecutioner.
 *
 * @see org.apache.commons.dbutils.QueryRunner
 */
@Slf4j
public class DBTransactionExecutioner {
  private final Connection _conn;
  private final QueryRunner _queryRunner;

  public DBTransactionExecutioner(QueryRunner queryRunner, Connection conn) {
    _conn = conn;
    _queryRunner = queryRunner;
  }

  /**
   * returns the last id from a previous insert statement. Note that last insert and this operation
   * should use the same connection.
   *
   * @return the last inserted id in mysql per connection.
   */
  public long getLastInsertId() throws SQLException {
    // A default connection: autocommit = true.
    long num;
    try {
      num = ((Number) _queryRunner
          .query(_conn, "SELECT LAST_INSERT_ID();", new ScalarHandler<>(1)))
          .longValue();
    } catch (SQLException ex) {
      log.error("Cannot get last insertion ID");
      throw ex;
    }
    return num;
  }

  public <T> T query(String querySql, ResultSetHandler<T> resultHandler, Object... params)
      throws SQLException {
    try {
      return _queryRunner.query(_conn, querySql, resultHandler, params);
    } catch (final SQLException ex) {
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }

  public int update(String updateClause, Object... params) throws SQLException {
    try {
      return _queryRunner.update(_conn, updateClause, params);
    } catch (SQLException ex) {
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }
}
