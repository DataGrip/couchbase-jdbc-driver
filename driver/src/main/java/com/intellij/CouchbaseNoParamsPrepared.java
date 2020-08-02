package com.intellij;

import org.jetbrains.annotations.NotNull;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class CouchbaseNoParamsPrepared implements PreparedStatement {
    private final String sql;
    private final CouchbaseStatement delegateStatement;

    public CouchbaseNoParamsPrepared(@NotNull String sql, @NotNull CouchbaseStatement statement) {
        this.sql = sql;
        this.delegateStatement = statement;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return delegateStatement.executeQuery(sql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return delegateStatement.executeUpdate(sql);
    }

    @Override
    public boolean execute() throws SQLException {
        return delegateStatement.execute(sql);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void clearParameters() throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters are not supported for this statements");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Method should not be called on prepared statement");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Method should not be called on prepared statement");
    }

    @Override
    public void close() throws SQLException {
        delegateStatement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return delegateStatement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        delegateStatement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return delegateStatement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        delegateStatement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        delegateStatement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return delegateStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        delegateStatement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        delegateStatement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegateStatement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegateStatement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        delegateStatement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("Method should not be called on prepared statement");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return delegateStatement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return delegateStatement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return delegateStatement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        delegateStatement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return delegateStatement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        delegateStatement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return delegateStatement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return delegateStatement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return delegateStatement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
       throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return delegateStatement.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return delegateStatement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return delegateStatement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Method should not be called on prepared statement");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Method should not be called on prepared statement");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Method should not be called on prepared statement");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Method should not be called on prepared statement");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Method should not be called on prepared statement");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Method should not be called on prepared statement");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return delegateStatement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegateStatement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        delegateStatement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return delegateStatement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        delegateStatement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return delegateStatement.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
