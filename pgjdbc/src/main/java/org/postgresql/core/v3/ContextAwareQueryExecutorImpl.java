/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3;

import org.postgresql.core.PGStream;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.ResultHandler;
import org.postgresql.jdbc.BatchResultHandler;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class ContextAwareQueryExecutorImpl extends QueryExecutorImpl {

  private final Context context = new Context("", 0, null);

  public ContextAwareQueryExecutorImpl(
      PGStream pgStream,
      int cancelSignalTimeout,
      Properties info)
      throws SQLException, IOException {
    super(pgStream, cancelSignalTimeout, info);
  }

  @Override
  public void execute(
      Query[] queries,
      @Nullable ParameterList[] parameterLists,
      BatchResultHandler batchHandler,
      int maxRows,
      int fetchSize,
      int flags,
      boolean adaptiveFetch) throws SQLException {
    super.execute(queries, parameterLists, batchHandler, maxRows, fetchSize, flags, adaptiveFetch);
    context.sqlCount += queries.length;
  }

  public void setContextUserInfo(Object userInfo) {
    context.userInfo = userInfo;
  }

  @Override
  public void execute(
      Query query,
      @Nullable ParameterList parameters,
      ResultHandler handler,
      int maxRows,
      int fetchSize,
      int flags,
      boolean adaptiveFetch) throws SQLException {
    super.execute(query, parameters, handler, maxRows, fetchSize, flags, adaptiveFetch);
    context.sqlCount++;
  }

  public Context getContext() {
    return context.copy();
  }

  public void resetContext(String sessionOwnerIdentifier) {
    context.sessionOwnerIdentifier = sessionOwnerIdentifier;
    context.sqlCount = 0;
  }

  public static class Context {
    private String sessionOwnerIdentifier = "";
    private int sqlCount;
    private Object userInfo;

    public Context(String sessionOwnerIdentifier, int sqlCount, Object userInfo) {
      this.sessionOwnerIdentifier = sessionOwnerIdentifier;
      this.sqlCount = sqlCount;
      this.userInfo = userInfo;
    }

    private Context(Context context) {
      this.sessionOwnerIdentifier = context.sessionOwnerIdentifier;
      this.sqlCount = context.sqlCount;
      this.userInfo = context.userInfo;
    }

    public String getSessionOwnerIdentifier() {
      return sessionOwnerIdentifier;
    }

    public void setSessionOwnerIdentifier(String sessionOwnerIdentifier) {
      this.sessionOwnerIdentifier = sessionOwnerIdentifier;
    }

    public Object getUserInfo() {
      return userInfo;
    }

    public int getSqlCount() {
      return sqlCount;
    }

    private Context copy() {
      return new Context(this);
    }

  }

}
