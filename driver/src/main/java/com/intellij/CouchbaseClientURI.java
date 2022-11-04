package com.intellij;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.*;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.intellij.DriverPropertyInfoHelper.*;

class CouchbaseClientURI {
    static final String PREFIX = "jdbc:couchbase:";
    private static final String HTTP_SCHEMA = "couchbase://";
    private static final String HTTPS_SCHEMA = "couchbases://";

    private static final Set<String> JDBC_KEYS = new HashSet<>(ContainerUtil.map(
            Arrays.asList(USER, PASSWORD, ENABLE_SSL, VERIFY_SERVER_CERTIFICATE, DEFAULT_BUCKET),
            key -> key.toLowerCase(Locale.ENGLISH)));

    private final String connectionString;
    private final String uri;
    private final String hosts;
    private final String userName;
    private final String password;
    private final String defaultBucket;
    private final boolean sslEnabled;
    private final boolean verifyServerCert;

    public CouchbaseClientURI(@NotNull String uri, @Nullable Properties info) {
        this.uri = uri;
        if (!uri.startsWith(PREFIX)) {
            throw new IllegalArgumentException("URI needs to start with " + PREFIX);
        }

        String trimmedUri = uri.substring(PREFIX.length());
        Map<String, List<String>> options = null;
        String serverPart;
        String nsPart = null;

        int optionsStartIndex = trimmedUri.indexOf("?");
        if (optionsStartIndex >= 0) {
            serverPart = trimmedUri.substring(0, optionsStartIndex);
            options = parseOptions(trimmedUri.substring(optionsStartIndex + 1));
        } else {
            serverPart = trimmedUri;
        }

        int lastSlashIndex = serverPart.lastIndexOf("/");
        if (lastSlashIndex >= 0) {
            nsPart = serverPart.substring(lastSlashIndex + 1);
            serverPart = serverPart.substring(0, lastSlashIndex);
        }

        setLoggingLevel(info, options);

        this.userName = getOption(info, options, USER, null);
        this.password = getOption(info, options, PASSWORD, null);
        this.sslEnabled = isTrue(getOption(info, options, ENABLE_SSL, ENABLE_SSL_DEFAULT));
        this.verifyServerCert = isTrue(getOption(info, options, VERIFY_SERVER_CERTIFICATE,
                VERIFY_SERVER_CERTIFICATE_DEFAULT));
        this.hosts = serverPart;
        this.defaultBucket = nsPart != null && !nsPart.isEmpty() ? nsPart : getOption(info, options, DEFAULT_BUCKET, null);
        this.connectionString = createConnectionString(serverPart, options);
    }

    private void setLoggingLevel(Properties info, Map<String, List<String>> options) {
        Logger logger = Logger.getLogger("com.couchbase.client");
        String logLevel = getOption(info, options, LOGGING_LEVEL, LOGGING_LEVEL_DEFAULT);
        if (logLevel == null) return;
        Level level;
        try {
            level = Level.parse(logLevel.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return;
        }
        logger.setLevel(level);
        for (Handler h : logger.getParent().getHandlers()) {
            if (h instanceof ConsoleHandler) {
                h.setLevel(level);
            }
        }
    }

    /**
     * @return option from properties or from uri if it is not found in properties.
     * null if options was not found.
     */
    @Nullable
    private String getOption(@Nullable Properties properties, @Nullable Map<String, List<String>> options,
                             @NotNull String optionName, @Nullable String defaultValue) {
        if (properties != null) {
            String option = (String) properties.get(optionName);
            if (option != null) {
                return option;
            }
        }
        String value = getLastValue(options, optionName);
        return value != null ? value : defaultValue;
    }

    ClusterConnection createClusterConnection() throws SQLException {
        String connectionStringWithSchema = (sslEnabled ? HTTPS_SCHEMA : HTTP_SCHEMA) + connectionString;
        ClusterEnvironment.Builder builder = ClusterEnvironment.builder()
                .load(new ConnectionStringPropertyLoader(connectionStringWithSchema));
        Authenticator authenticator = authenticate(builder);
        ClusterEnvironment environment = builder.build();
        ClusterConnection clusterConnection = new ClusterConnection(
                Cluster.connect(connectionStringWithSchema, ClusterOptions
                        .clusterOptions(authenticator)
                        .environment(environment)
                ), environment);
        clusterConnection.initConnection(defaultBucket);
        return clusterConnection;
    }

    private Authenticator authenticate(ClusterEnvironment.Builder envBuilder) throws SQLException {
        if (sslEnabled) {
            SecurityConfig.Builder securityConfig = SecurityConfig.enableTls(true);
            if (verifyServerCert) {
                SslKeyStoreConfig trustStore = SslKeyStoreConfig.create(SslKeyStoreConfig.Type.TRUST_STORE);
                envBuilder.securityConfig(securityConfig.trustStore(trustStore.getPath(), trustStore.getPassword(),
                        trustStore.getType()));
            } else {
                envBuilder.securityConfig(securityConfig.trustManagerFactory(InsecureTrustManagerFactory.INSTANCE));
            }

            if (userName == null || userName.isEmpty()) {
                SslKeyStoreConfig keyStore = SslKeyStoreConfig.create(SslKeyStoreConfig.Type.KEY_STORE);
                return CertificateAuthenticator.fromKeyStore(
                        keyStore.getPath(), keyStore.getPassword(), keyStore.getType());
            }
        }

        if (userName == null || userName.isEmpty() || password == null) {
            throw new SQLException("Username or password is not provided");
        }
        return PasswordAuthenticator.create(userName, password);
    }

    @Nullable
    private String getLastValue(@Nullable Map<String, List<String>> optionsMap, @NotNull String key) {
        if (optionsMap == null) return null;
        String normalizedKey = key.toLowerCase(Locale.ENGLISH);
        List<String> valueList = optionsMap.get(normalizedKey);
        if (valueList == null || valueList.size() == 0) return null;
        return valueList.get(valueList.size() - 1);
    }

    @NotNull
    private Map<String, List<String>> parseOptions(@NotNull String optionsPart) {
        Map<String, List<String>> optionsMap = new HashMap<>();

        for (String _part : optionsPart.split("&")) {
            int idx = _part.indexOf("=");
            if (idx >= 0) {
                String key = _part.substring(0, idx).toLowerCase(Locale.ENGLISH);
                String value = _part.substring(idx + 1);
                List<String> valueList = optionsMap.get(key);
                if (valueList == null) {
                    valueList = new ArrayList<>(1);
                }
                valueList.add(value);
                optionsMap.put(key, valueList);
            }
        }

        return optionsMap;
    }

    @NotNull
    private String createConnectionString(@NotNull String hosts, @Nullable Map<String, List<String>> optionsMap) {
        if (optionsMap == null) {
            return hosts;
        }
        return optionsMap.keySet().stream()
                .filter(key -> !JDBC_KEYS.contains(key))
                .map(key -> key + "=" + getLastValue(optionsMap, key))
                .collect(Collectors.joining("&", hosts + "?", ""));
    }

    // ---------------------------------

    /**
     * Gets the username
     *
     * @return the username
     */
    public String getUsername() {
        return userName;
    }

    /**
     * Gets the password
     *
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Gets the ssl enabled property
     *
     * @return the ssl enabled property
     */
    public Boolean getSslEnabled() {
        return sslEnabled;
    }

    /**
     * @return verifyServerCertificate property
     */
    public Boolean getVerifyServerCertificate() {
        return verifyServerCert;
    }

    /**
     * Gets the list of hosts and params sent directly to Java SDK
     *
     * @return the host list
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * Gets the list of hosts
     *
     * @return the host list
     */
    public String getHosts() {
        return hosts;
    }

    /**
     * Get the unparsed URI.
     *
     * @return the URI
     */
    public String getURI() {
        return uri;
    }

    /**
     * Gets the default bucket
     *
     * @return the default bucket
     */
    public String getDefaultBucket() {
        return defaultBucket;
    }

    @Override
    public String toString() {
        return uri;
    }
}
