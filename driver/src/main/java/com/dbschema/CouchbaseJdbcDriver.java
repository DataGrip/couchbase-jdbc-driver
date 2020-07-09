
package com.dbschema;

import com.couchbase.client.java.Cluster;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static com.dbschema.CouchbaseClientURI.PREFIX;


/**
 * Minimal implementation of the JDBC standards for the Couchbase database.
 */
public class CouchbaseJdbcDriver implements Driver {
    private static final String RETURN_NULL_STRINGS_FROM_INTRO_QUERY_KEY = "couchbase.jdbc.return.null.strings.from.intro.query";

    static {
        try {
            DriverManager.registerDriver(new CouchbaseJdbcDriver());
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * todo: change this syntax to more suitable for Couchbase
     * Connect to the database using a URL like :
     * jdbc:couchbase:host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
     * The URL's hosts and ports configuration is passed as it is to the Couchbase native Java driver.
     */
    public Connection connect(String url, Properties info) throws SQLException {
        if (url != null && acceptsURL(url)) {
            CouchbaseClientURI clientURI = new CouchbaseClientURI(url, info);
            try {
                Cluster cluster = clientURI.createCluster();
                boolean returnNullStringsFromIntroQuery =
                        Boolean.parseBoolean(info.getProperty(RETURN_NULL_STRINGS_FROM_INTRO_QUERY_KEY));
                return new CouchbaseConnection(cluster, this, returnNullStringsFromIntroQuery);
            } catch (Exception e) {
                throw new SQLException(e.getMessage(), e);
            }
        }
        return null;
    }

    /**
     * URLs accepted are of the form: jdbc:couchbase:host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
     */
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return DriverPropertyInfoHelper.getPropertyInfo();
    }

    String getVersion() {
        return "0.2.0";
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 2;
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
