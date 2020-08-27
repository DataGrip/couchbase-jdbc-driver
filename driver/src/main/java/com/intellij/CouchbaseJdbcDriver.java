package com.intellij;

import com.couchbase.client.java.Cluster;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static com.intellij.CouchbaseClientURI.PREFIX;


/**
 * Minimal implementation of the JDBC standards for the Couchbase database.
 */
public class CouchbaseJdbcDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new CouchbaseJdbcDriver());
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Connect to the database using a URL like :
     * jdbc:couchbase:host1[:port1][,host2[:port2],...][?option=value[&option=value&...]]
     * The URL's hosts and ports configuration is passed as it is to the Couchbase native Java driver.
     */
    public Connection connect(@NotNull String url, @Nullable Properties info) throws SQLException {
        if (acceptsURL(url)) {
            try {
                CouchbaseClientURI clientURI = new CouchbaseClientURI(url, info);
                ClusterConnection cluster = clientURI.createClusterConnection();
                if (info == null) {
                    info = new Properties();
                }
                return new CouchbaseConnection(cluster, this, clientURI, info);
            } catch (Exception e) {
                throw new SQLException(e.getMessage(), e);
            }
        }
        return null;
    }

    /**
     * URLs accepted are of the form:
     * jdbc:couchbase:host1[:port1][,host2[:port2],...][?option=value[&option=value&...]]
     */
    @Override
    public boolean acceptsURL(@NotNull String url) {
        return url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return DriverPropertyInfoHelper.getPropertyInfo();
    }

    String getVersion() {
        return "0.3.0";
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 3;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
