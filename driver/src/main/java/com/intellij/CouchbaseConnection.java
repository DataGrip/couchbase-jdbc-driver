package com.intellij;

import com.couchbase.client.java.Cluster;
import org.jetbrains.annotations.NotNull;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class CouchbaseConnection implements Connection {
    private static final String RETURN_NULL_STRINGS_FROM_INTRO_QUERY_KEY =
            "couchbase.jdbc.return.null.strings.from.intro.query";

    private final Cluster cluster;
    private final CouchbaseJdbcDriver driver;
    private final CouchbaseClientURI uri;
    private final Properties properties;
    private boolean isClosed = false;
    private boolean isReadOnly = false;

    CouchbaseConnection(@NotNull Cluster cluster, @NotNull CouchbaseJdbcDriver couchbaseJdbcDriver,
                        @NotNull CouchbaseClientURI uri, @NotNull Properties properties) {
        this.cluster = cluster;
        this.driver = couchbaseJdbcDriver;
        this.uri = uri;
        this.properties = properties;
    }

    public String getCatalog() throws SQLException {
        checkClosed();
        return null;
    }

    Cluster getCluster() {
        return cluster;
    }

    CouchbaseClientURI getUri() {
        return uri;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        try {
            return new CouchbaseStatement(cluster);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        try {
            boolean returnNullStringsFromIntroQuery =
                    Boolean.parseBoolean(properties.getProperty(RETURN_NULL_STRINGS_FROM_INTRO_QUERY_KEY));
            return new CouchbasePreparedStatement(cluster, sql, returnNullStringsFromIntroQuery);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Couchbase does not support SQL natively.");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
    }

    @Override
    public void close() {
        if (!isClosed) {
            cluster.disconnect();
        }
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new CouchbaseMetaData(this, driver);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        isReadOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return isReadOnly;
    }

    @Override
    public void setCatalog(String catalog) {

    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        // Since the only valid value for MongDB is Connection.TRANSACTION_NONE, and the javadoc for this method
        // indicates that this is not a valid value for level here, throw unsupported operation exception.
        throw new UnsupportedOperationException("Couchbase provides no support for transactions.");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) {
    }

    @Override
    public void setClientInfo(Properties properties) {
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();

        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        return null;
    }


    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Statement was previously closed.");
        }
    }

    @Override
    public void setSchema(String schema) {
        setCatalog(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return getCatalog();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
