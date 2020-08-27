package com.intellij;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.sql.SQLException;
import java.util.Optional;
import java.util.logging.Logger;

class SslKeyStoreConfig {
    private final String storeType;
    private final String storePassword;
    private final String storeUrl;

    private SslKeyStoreConfig(String storeType, String storePassword, String storeUrl) {
        this.storeType = storeType;
        this.storePassword = storePassword;
        this.storeUrl = storeUrl;
    }

    public static SslKeyStoreConfig create(Type type) throws SQLException {
        String storeType = System.getProperty("javax.net.ssl." + type + "StoreType", KeyStore.getDefaultType());
        String storePassword = System.getProperty("javax.net.ssl." + type + "StorePassword");
        String storeUrl = System.getProperty("javax.net.ssl." + type + "Store", "");
        if (storeUrl == null || storeUrl.isEmpty()) {
            throw new SQLException(type + "Store url is not provided");
        }
        return new SslKeyStoreConfig(storeType, storePassword, storeUrl);
    }

    public Optional<String> getType() {
        return Optional.ofNullable(storeType);
    }

    public String getPassword() {
        return storePassword;
    }

    public Path getPath() {
        return Paths.get(storeUrl);
    }

    public enum Type {
        KEY_STORE("key"), TRUST_STORE("trust");

        private final String key;

        Type(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return key;
        }
    }
}
