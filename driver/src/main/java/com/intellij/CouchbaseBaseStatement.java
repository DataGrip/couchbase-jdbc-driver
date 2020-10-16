package com.intellij;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.intellij.resultset.CouchbaseReactiveResultSet;
import com.intellij.resultset.CouchbaseSimpleResultSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;

import java.sql.*;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

import static com.intellij.DriverPropertyInfoHelper.ScanConsistency.getQueryScanConsistency;

@SuppressWarnings("RedundantThrows")
public abstract class CouchbaseBaseStatement implements Statement {
    protected final Cluster cluster;
    protected final Properties properties;
    protected final boolean isReadOnly;
    protected CouchbaseConnection connection;
    protected ResultSet result;
    private int fetchSize = Queues.SMALL_BUFFER_SIZE;
    private boolean isClosed = false;
    private int updateCount = -1;

    CouchbaseBaseStatement(@NotNull CouchbaseConnection connection) {
        this.connection = connection;
        this.properties = connection.getProperties();
        this.cluster = connection.getCluster();
        this.isReadOnly = connection.isReadOnly();
    }

    protected QueryOptions makeQueryOptions() {
        return QueryOptions.queryOptions()
                .scanConsistency(getQueryScanConsistency(properties))
                .readonly(isReadOnly)
                .metrics(true);
    }

    @Override
    public void close() throws SQLException {
        if (result != null) {
            result.close();
        }
        result = null;
        connection = null;
        isClosed = true;
    }

    void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Statement was previously closed.");
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    protected boolean executeInner(@NotNull String sql, @NotNull Mono<ReactiveQueryResult> resultMono) throws SQLException {
        try {
            ReactiveQueryResult result = Objects.requireNonNull(resultMono.block(), "Query did not return result");
            ResultSet resultSet;
            if (sql.toLowerCase(Locale.ENGLISH).startsWith("infer")) resultSet = listResultSet(result);
            else {
                resultSet = new CouchbaseReactiveResultSet(this, result);
                long mutationCount = ((CouchbaseReactiveResultSet) resultSet).getMutationCount();
                if (mutationCount != -1) {
                    resultSet.close();
                    setNewResultSet(resultSet, mutationCount);
                    return false;
                }
            }
            setNewResultSet(resultSet);
            return true;
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    @Nullable
    private ResultSet listResultSet(@NotNull ReactiveQueryResult result) {
        List<Object> list = result.rowsAs(JsonArray.class)
            .map(JsonArray::toList)
            .blockFirst();
        return list == null ? null : new CouchbaseSimpleResultSet(list);
    }

    protected void setNewResultSet(@Nullable ResultSet resultSet) throws SQLException {
        this.setNewResultSet(resultSet, -1);
    }

    protected void setNewResultSet(@Nullable ResultSet resultSet, long updateCount) throws SQLException {
        if (result != null) {
            result.close();
        }
        result = resultSet;
        this.updateCount = coalesceInt(updateCount);
    }

    private int coalesceInt(long value) {
        if (value < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int)value;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        int returnCount = updateCount;
        updateCount = -1;
        return returnCount;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return result;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        // todo
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Couchbase provides no support for interrupting an operation.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null; // todo
    }

    @Override
    public void clearWarnings() throws SQLException {
        // todo
    }

    @Override
    public void setCursorName(final String name) throws SQLException {
        checkClosed();
        // Driver doesn't support positioned updates for now, so no-op.
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() {
        return this.fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
