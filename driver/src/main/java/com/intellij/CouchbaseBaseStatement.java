package com.intellij;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.intellij.resultset.CouchbaseReactiveResultSet;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Objects;
import java.util.Properties;

import static com.intellij.DriverPropertyInfoHelper.ScanConsistency.getQueryScanConsistency;

public abstract class CouchbaseBaseStatement implements Statement {
    protected final Cluster cluster;
    protected final Properties properties;
    protected final boolean isReadOnly;
    protected ResultSet result;
    private int fetchSize = Queues.SMALL_BUFFER_SIZE;
    private boolean isClosed = false;

    CouchbaseBaseStatement(@NotNull Cluster cluster, @NotNull Properties properties, boolean isReadOnly) {
        this.properties = properties;
        this.cluster = cluster;
        this.isReadOnly = isReadOnly;
    }

    protected QueryOptions makeQueryOptions() {
        return QueryOptions.queryOptions()
                .scanConsistency(getQueryScanConsistency(properties))
                .readonly(isReadOnly);
    }

    @Override
    public void close() throws SQLException {
        if (result != null) {
            result.close();
        }
        result = null;
        isClosed = true;
    }

    void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Statement was previously closed.");
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    protected boolean executeInner(Mono<ReactiveQueryResult> resultMono) throws SQLException {
        try {
            ReactiveQueryResult result = resultMono.block();
            Objects.requireNonNull(result, "Query did not return result");
            setNewResultSet(new CouchbaseReactiveResultSet(this, result));
            return true;
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    protected void setNewResultSet(ResultSet resultSet) throws SQLException {
        if (this.result != null) {
            this.result.close();
        }
        this.result = resultSet;
    }

    @Override
    public boolean getMoreResults() {
        return false;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return -1;
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
    public void setMaxRows(int max) {
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
    public SQLWarning getWarnings() {
        return null; // todo
    }

    @Override
    public void clearWarnings() {
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
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {
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
