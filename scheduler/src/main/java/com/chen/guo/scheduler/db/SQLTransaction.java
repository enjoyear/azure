package com.chen.guo.scheduler.db;

import java.sql.SQLException;


/**
 * This interface defines how a sequence of sql statements are organized and packed together.
 * All transaction implementations must follow this interface, and will be called in {@link
 * DBQueryExecutioner#transaction(SQLTransaction)}
 *
 * @param <T> The transaction return type
 */
@FunctionalInterface
public interface SQLTransaction<T> {
  T execute(DBTransactionExecutioner transOperator) throws SQLException;
}
