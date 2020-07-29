package com.intellij;

import com.couchbase.client.java.json.JsonObject;
import com.intellij.meta.ColumnInfo;
import com.intellij.meta.TableInfo;
import com.intellij.resultset.CouchbaseListResultSet;
import com.intellij.resultset.CouchbaseResultSetMetaData;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.*;

import static com.intellij.EscapingUtil.stripBackquotes;
import static com.intellij.resultset.CouchbaseResultSetMetaData.createColumn;

/**
 * Couchbase namespaces are equivalent to catalogs for this driver. Schemas aren't used. Couchbase buckets are
 * equivalent to tables, in that each bucket is a table.
 */
@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class CouchbaseMetaData implements DatabaseMetaData {

    private static final String DB_NAME = "Couchbase";

    private final CouchbaseConnection connection;
    private final CouchbaseJdbcDriver driver;

    CouchbaseMetaData(@NotNull CouchbaseConnection connection, @NotNull CouchbaseJdbcDriver driver) {
        this.connection = connection;
        this.driver = driver;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() {
        return CouchbaseListResultSet.empty();
    }

    public ResultSet getTables(String catalogName, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException{
        String sql = "SELECT null AS TABLE_CAT, namespace_id AS TABLE_SCHEM, name AS TABLE_NAME, " +
                "'TABLE' AS TABLE_TYPE, null AS REMARKS, null AS TYPE_CAT, null AS TYPE_SCHEM, null AS TYPE_NAME, " +
                "null AS SELF_REFERENCING_COL_NAME, null AS REF_GENERATION FROM system:keyspaces";
        if (schemaPattern != null || tableNamePattern != null) {
            sql += " WHERE ";
            if (schemaPattern != null) {
                sql += "namespace_id LIKE '" + schemaPattern + "'";
            }
            if (schemaPattern != null && tableNamePattern != null) {
                sql += " AND ";
            }
            if (tableNamePattern != null) {
                sql += "name LIKE '" + tableNamePattern +"'";
            }
        }
        sql += " ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME";

        try (CouchbaseStatement statement = connection.createStatement()) {
            CouchbaseListResultSet resultSet = statement.executeMetaQuery(sql);
            if (CouchbaseSqlLikePattern.create(schemaPattern).matches("system")) {
                resultSet.addRows(getSystemKeyspaces());
            }
            resultSet.setMetadata(new CouchbaseResultSetMetaData(Arrays.asList(
                    createColumn("TABLE_CAT", "string"),
                    createColumn("TABLE_SCHEM", "string"),
                    createColumn("TABLE_NAME", "string"),
                    createColumn("TABLE_TYPE", "string"),
                    createColumn("REMARKS", "string"),
                    createColumn("TYPE_CAT", "string"),
                    createColumn("TYPE_SCHEM", "string"),
                    createColumn("TYPE_NAME", "string"),
                    createColumn("SELF_REFERENCING_COL_NAME", "string"),
                    createColumn("REF_GENERATION", "string")
            )));
            return resultSet;
        }
    }

    private static List<Map<String, Object>> getSystemKeyspaces() {
        List<Map<String, Object>> rows = new ArrayList<>();
        List<String> keyspaceNames = Arrays.asList(
                "dual", "datastores", "namespaces", "keyspaces", "indexes",
                "prepareds", "completed_requests", "active_requests",
                "my_user_info", "user_info", "nodes", "applicable_roles");
        for (String name : keyspaceNames) {
            Map<String, Object> row = new HashMap<>(10);
            row.put("TABLE_CAT", null);
            row.put("TABLE_SCHEM", "system");
            row.put("TABLE_NAME", name);
            row.put("TABLE_TYPE", "TABLE");
            row.put("REMARKS", null);
            row.put("TYPE_CAT", null);
            row.put("TYPE_SCHEM", null);
            row.put("TYPE_NAME", null);
            row.put("SELF_REFERENCING_COL_NAME", null);
            row.put("REF_GENERATION", null);
            rows.add(row);
        }
        return rows;
    }

    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern, String columnNamePattern) throws SQLException {
        CouchbaseDocumentsSampler sampler = new CouchbaseDocumentsSampler(connection);
        try (ResultSet tables = getTables(catalog, schemaPattern, tableNamePattern, null)) {
            CouchbaseListResultSet listResultSet = new CouchbaseListResultSet(getColumnsInfoRows(sampler, tables));
            listResultSet.setMetadata(createColumnsMeta());
            return listResultSet;
        }
    }

    private static List<Map<String, Object>> getColumnsInfoRows(CouchbaseDocumentsSampler sampler,
                                                                ResultSet tablesRs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (TableInfo table : getTablesList(tablesRs)) {
            populateColumnsResultSet(rows, sampler.sample(table), table);
        }
        return rows;
    }

    private static List<TableInfo> getTablesList(ResultSet resultSet) throws SQLException {
        List<TableInfo> tables = new ArrayList<>();
        while (resultSet.next()) {
            String tableName = resultSet.getString("TABLE_NAME");
            String tableSchema = resultSet.getString("TABLE_SCHEM");
            if (tableName != null) {
                tables.add(new TableInfo(tableName, tableSchema));
            }
        }
        return tables;
    }

    private static void populateColumnsResultSet(List<Map<String, Object>> rs, Collection<ColumnInfo> columns,
                                                 TableInfo table) {
        for (ColumnInfo column : columns) {
            Map<String, Object> row = new HashMap<>(23);
            row.put("TABLE_CAT", null);
            row.put("TABLE_SCHEM", table.getSchema());
            row.put("TABLE_NAME", table.getName());
            row.put("COLUMN_NAME", column.getName());
            row.put("DATA_TYPE", column.getType());
            row.put("TYPE_NAME", column.getTypeName());
            row.put("COLUMN_SIZE", null);
            row.put("BUFFER_LENGTH", null);
            row.put("DECIMAL_DIGITS", null);
            row.put("NUM_PREC_RADIX", null);
            row.put("NULLABLE", columnNullable);
            row.put("REMARKS", null);
            row.put("COLUMN_DEF", null);
            row.put("SQL_DATA_TYPE", null);
            row.put("SQL_DATETIME_SUB", null);
            row.put("CHAR_OCTET_LENGTH", null);
            row.put("ORDINAL_POSITION", null);
            row.put("IS_NULLABLE", "YES");
            row.put("SCOPE_CATLOG", null);
            row.put("SCOPE_SCHEMA", null);
            row.put("SCOPE_TABLE", null);
            row.put("SOURCE_DATA_TYPE", null);
            row.put("IS_AUTOINCREMENT", "NO");
            rs.add(row);
        }
    }

    private static CouchbaseResultSetMetaData createColumnsMeta() {
        return new CouchbaseResultSetMetaData(Arrays.asList(
                createColumn("TABLE_CAT", "string"),
                createColumn("TABLE_SCHEM", "string"),
                createColumn("TABLE_NAME", "string"),
                createColumn("COLUMN_NAME", "string"),
                createColumn("DATA_TYPE", "numeric"),
                createColumn("TYPE_NAME", "numeric"),
                createColumn("COLUMN_SIZE", "numeric"),
                createColumn("BUFFER_LENGTH", "numeric"),
                createColumn("DECIMAL_DIGITS", "numeric"),
                createColumn("NUM_PREC_RADIX", "numeric"),
                createColumn("NULLABLE", "numeric"),
                createColumn("REMARKS", "string"),
                createColumn("COLUMN_DEF", "string"),
                createColumn("SQL_DATA_TYPE", "numeric"),
                createColumn("SQL_DATETIME_SUB", "numeric"),
                createColumn("CHAR_OCTET_LENGTH", "numeric"),
                createColumn("ORDINAL_POSITION", "numeric"),
                createColumn("IS_NULLABLE", "string"),
                createColumn("SCOPE_CATLOG", "string"),
                createColumn("SCOPE_SCHEMA", "string"),
                createColumn("SCOPE_TABLE", "string"),
                createColumn("SOURCE_DATA_TYPE", "numeric"),
                createColumn("IS_AUTOINCREMENT", "string")
        ));
    }

    public ResultSet getPrimaryKeys(String catalogName, String schemaName, String tableNamePattern)
            throws SQLException {
        String sql = "SELECT null AS TABLE_CAT, namespace_id AS TABLE_SCHEM, keyspace_id AS TABLE_NAME, " +
                "'id' AS COLUMN_NAME, 1 AS KEY_SEQ, name AS PK_NAME FROM system:indexes WHERE is_primary = true";
        if (schemaName != null) {
            sql += " AND namespace_id LIKE '" + schemaName + "'";
        }
        if (tableNamePattern != null) {
            sql += " AND keyspace_id LIKE '" + tableNamePattern + "'";
        }
        sql += " ORDER BY COLUMN_NAME, TABLE_SCHEM, TABLE_NAME";

        try (CouchbaseStatement statement = connection.createStatement()) {
            CouchbaseListResultSet listResultSet = statement.executeMetaQuery(sql);
            listResultSet.setMetadata(new CouchbaseResultSetMetaData(Arrays.asList(
                    createColumn("TABLE_CAT", "string"),
                    createColumn("TABLE_SCHEM", "string"),
                    createColumn("TABLE_NAME", "string"),
                    createColumn("COLUMN_NAME", "string"),
                    createColumn("KEY_SEQ", "short"),
                    createColumn("PK_NAME", "string")
            )));
            return listResultSet;
        }
    }

    public ResultSet getIndexInfo(String catalogName, String schemaName, String tableNamePattern, boolean unique,
                                  boolean approximate) throws SQLException {
        String sql = "SELECT namespace_id, keyspace_id, name, index_key, is_primary, `condition` FROM system:indexes";
        if (tableNamePattern != null || schemaName != null || unique) {
            sql += " WHERE";
            List<String> clauses = new ArrayList<>();
            if (tableNamePattern != null) {
                clauses.add(" keyspace_id LIKE '" + tableNamePattern + "'");
            }
            if (schemaName != null) {
                clauses.add(" namespace_id LIKE '" + schemaName + "'");
            }
            if (unique) {
                clauses.add(" is_primary = true");
            }
            sql += String.join(" AND ", clauses);
        }
        sql += " ORDER BY name";

        try (CouchbaseStatement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                CouchbaseListResultSet listResultSet = new CouchbaseListResultSet(getIndexInfoRows(resultSet));
                listResultSet.setMetadata(createIndexMeta());
                return listResultSet;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getIndexInfoRows(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> resultRows = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, Object> row = (Map<String, Object>) resultSet.getObject(1);
            List<?> indexKeys = (List<?>) row.get("index_key");
            if (indexKeys == null || indexKeys.size() == 0) {
                resultRows.add(createIndexInfoCol(row.get("namespace_id"), row.get("keyspace_id"), row.get("name"),
                        "id", 1, row.get("is_primary"), row.get("condition")));
            } else {
                int ordinal = 1;
                for (Object indexKey : indexKeys) {
                    resultRows.add(createIndexInfoCol(row.get("namespace_id"), row.get("keyspace_id"), row.get("name"),
                            indexKey.toString(), ordinal, row.get("is_primary"), row.get("condition")));
                    ordinal++;
                }
            }
        }
        return resultRows;
    }

    private static Map<String, Object> createIndexInfoCol(Object namespaceId, Object keyspaceId, Object name,
                                                          String columnName, int ordinal, Object isPrimary,
                                                          Object filterCondition) {
        String sortingDirection = "A";
        if (columnName.endsWith("DESC")) {
            sortingDirection = "D";
            columnName = columnName.substring(0, columnName.lastIndexOf("DESC")).trim();
        }
        Map<String, Object> indexCol = new HashMap<>(13);
        indexCol.put("TABLE_CAT", null);
        indexCol.put("TABLE_SCHEM", namespaceId);
        indexCol.put("TABLE_NAME", keyspaceId);
        indexCol.put("NON_UNIQUE", !Boolean.parseBoolean(String.valueOf(isPrimary)));
        indexCol.put("INDEX_QUALIFIER", null);
        indexCol.put("INDEX_NAME", name);
        indexCol.put("TYPE", tableIndexHashed);
        indexCol.put("ORDINAL_POSITION", ordinal);
        indexCol.put("COLUMN_NAME", stripBackquotes(columnName));
        indexCol.put("ASC_OR_DESC", sortingDirection);
        indexCol.put("CARDINALITY", 0);
        indexCol.put("PAGES", 0);
        indexCol.put("FILTER_CONDITION", filterCondition);
        return indexCol;
    }

    private static CouchbaseResultSetMetaData createIndexMeta() {
        return new CouchbaseResultSetMetaData(Arrays.asList(
                createColumn("TABLE_CAT", "string"),
                createColumn("TABLE_SCHEM", "string"),
                createColumn("TABLE_NAME", "string"),
                createColumn("NON_UNIQUE", "boolean"),
                createColumn("INDEX_QUALIFIER", "string"),
                createColumn("INDEX_NAME", "string"),
                createColumn("TYPE", "numeric"),
                createColumn("ORDINAL_POSITION", "numeric"),
                createColumn("COLUMN_NAME", "numeric"),
                createColumn("ASC_OR_DESC", "string"),
                createColumn("CARDINALITY", "numeric"),
                createColumn("PAGES", "numeric"),
                createColumn("FILTER_CONDITION", "string")
        ));
    }

    public ResultSet getTypeInfo() {
        return CouchbaseListResultSet.empty();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    public boolean allProceduresAreCallable() {
        return true;
    }

    public boolean allTablesAreSelectable() {
        return true;
    }

    public String getURL() {
        return connection.getUri().getURI();
    }

    public String getUserName() {
        return connection.getUri().getUsername();
    }

    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    public boolean nullsAreSortedHigh() {
        return false;
    }

    public boolean nullsAreSortedLow() {
        return true;
    }

    public boolean nullsAreSortedAtStart() {
        return false;
    }

    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    public String getDatabaseProductName() {
        return DB_NAME;
    }

    public String getDatabaseProductVersion() throws SQLException {
        List<JsonObject> results = connection.getCluster()
                .query("SELECT version() FROM system:dual;")
                .rowsAsObject();
        if (results.size() == 1) {
            JsonObject jsonObject = results.get(0);
            if (jsonObject.containsKey("$1")) {
                return parseVersion(String.valueOf(jsonObject.get("$1")));
            }
        }
        throw new SQLException("Unable to fetch database version");
    }

    private String parseVersion(String version) {
        String[] split = version.split("[.-]");
        if (split.length == 0) {
            return null;
        }
        if (split.length == 1) {
            return split[0];
        }
        return split[0] + "." + split[1];
    }

    public String getDriverName() {
        return "Couchbase JDBC Driver";
    }

    public String getDriverVersion() {
        return driver.getVersion();
    }

    public int getDriverMajorVersion() {
        return driver.getMajorVersion();
    }

    public int getDriverMinorVersion() {
        return driver.getMinorVersion();
    }

    public boolean usesLocalFiles() {
        return false;
    }

    public boolean usesLocalFilePerTable() {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    public String getIdentifierQuoteString() {
        return "`";
    }

    public String getSQLKeywords() {
        return null;
    }

    public String getNumericFunctions() {
        return null;
    }

    public String getStringFunctions() {
        return null;
    }

    public String getSystemFunctions() {
        return null;
    }

    public String getTimeDateFunctions() {
        return null;
    }

    public String getSearchStringEscape() {
        return "\\";
    }

    public String getExtraNameCharacters() {
        return "";
    }

    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    public boolean supportsColumnAliasing() {
        return true;
    }

    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    public boolean supportsConvert() {
        return false;
    }

    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    public boolean supportsTableCorrelationNames() {
        return true;
    }

    public boolean supportsDifferentTableCorrelationNames() {
        return true;
    }

    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    public boolean supportsOrderByUnrelated() {
        return false;
    }

    public boolean supportsGroupBy() {
        return true;
    }

    public boolean supportsGroupByUnrelated() {
        return true;
    }

    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    public boolean supportsLikeEscapeClause() {
        return false;
    }

    public boolean supportsMultipleResultSets() {
        return false;
    }

    public boolean supportsMultipleTransactions() {
        return false;
    }

    public boolean supportsNonNullableColumns() {
        return false;
    }

    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    public boolean supportsANSI92FullSQL() {
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    public boolean supportsOuterJoins() {
        return true;
    }

    public boolean supportsFullOuterJoins() {
        return false;
    }

    public boolean supportsLimitedOuterJoins() {
        return false;
    }

    public String getSchemaTerm() {
        return "namespace";
    }

    public String getProcedureTerm() {
        return "procedure";
    }

    public String getCatalogTerm() {
        return "namespace";
    }

    public boolean isCatalogAtStart() {
        return true;
    }

    public String getCatalogSeparator() {
        return ":";
    }

    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() {
        return true;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() {
        return true;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    public boolean supportsPositionedDelete() {
        return false;
    }

    public boolean supportsPositionedUpdate() {
        return false;
    }

    public boolean supportsSelectForUpdate() {
        return false;
    }

    public boolean supportsStoredProcedures() {
        return false;
    }

    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    public boolean supportsSubqueriesInExists() {
        return true;
    }

    public boolean supportsSubqueriesInIns() {
        return true;
    }

    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    public boolean supportsUnion() {
        return true;
    }

    public boolean supportsUnionAll() {
        return true;
    }

    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    public int getMaxCharLiteralLength() {
        return 0;
    }

    public int getMaxColumnNameLength() {
        return 0;
    }

    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    public int getMaxColumnsInIndex() {
        return 0;
    }

    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    public int getMaxColumnsInSelect() {
        return 0;
    }

    public int getMaxColumnsInTable() {
        return 0;
    }

    public int getMaxConnections() {
        return 0;
    }

    public int getMaxCursorNameLength() {
        return 0;
    }

    public int getMaxIndexLength() {
        return 0;
    }

    public int getMaxSchemaNameLength() {
        return 0;
    }

    public int getMaxProcedureNameLength() {
        return 0;
    }

    public int getMaxCatalogNameLength() {
        return 0;
    }

    public int getMaxRowSize() {
        return 0;
    }

    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    public int getMaxStatementLength() {
        return 0;
    }

    public int getMaxStatements() {
        return 0;
    }

    public int getMaxTableNameLength() {
        return 0;
    }

    public int getMaxTablesInSelect() {
        return 0;
    }

    public int getMaxUserNameLength() {
        return 0;
    }

    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    public boolean supportsTransactions() {
        //todo
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) {
        return Connection.TRANSACTION_NONE == level;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    public ResultSet getProcedures(String catalogName, String schemaPattern,
                                   String procedureNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getProcedureColumns(String catalogName, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getTableTypes() {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getColumnPrivileges(String catalogName, String schemaName,
                                         String table, String columnNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getTablePrivileges(String catalogName, String schemaPattern, String tableNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalogName, String schemaName, String table, int scope,
                                          boolean nullable) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getVersionColumns(String catalogName, String schemaName, String table) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getExportedKeys(String catalogName, String schemaName, String tableNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getImportedKeys(String catalogName, String schemaName, String tableNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        //todo
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalogName, String schemaPattern, String typeNamePattern, int[] types) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        //todo
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalogName, String schemaPattern, String typeNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getSuperTables(String catalogName, String schemaPattern, String tableNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getAttributes(String catalogName, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return Integer.parseInt((getDatabaseProductVersion().split("\\."))[0]);
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return Integer.parseInt((getDatabaseProductVersion().split("\\."))[1]);
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 2;
    }

    @Override
    public int getSQLStateType() {
        return DatabaseMetaData.sqlStateXOpen;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalogName, String schemaPattern) throws SQLException {
        String sql = "SELECT id AS TABLE_SCHEM, null AS TABLE_CATALOG FROM system:namespaces";
        if (schemaPattern != null) {
            sql += " WHERE name LIKE '" + schemaPattern + "'";
        }
        sql += " ORDER BY TABLE_CATALOG, TABLE_SCHEM";
        try (CouchbaseStatement statement = connection.createStatement()) {
            CouchbaseListResultSet resultSet = statement.executeMetaQuery(sql);
            if (CouchbaseSqlLikePattern.create(schemaPattern).matches("system")) {
                resultSet.addRows(getSystemSchemaInfo());
            }
            resultSet.setMetadata(new CouchbaseResultSetMetaData(Arrays.asList(
                    createColumn("TABLE_SCHEM", "string"),
                    createColumn("TABLE_CATALOG", "string")
            )));
            return resultSet;
        }
    }

    private static List<Map<String, Object>> getSystemSchemaInfo() {
        Map<String, Object> systemSchema = new HashMap<>();
        systemSchema.put("TABLE_SCHEM", "system");
        systemSchema.put("TABLE_CATALOG", null);
        return Collections.singletonList(systemSchema);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getFunctions(String catalogName, String schemaPattern, String functionNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getFunctionColumns(String catalogName, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public ResultSet getPseudoColumns(String catalogName, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) {
        return CouchbaseListResultSet.empty();
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }
}
