package com.intellij;

import com.couchbase.client.java.json.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * Couchbase namespaces are equivalent to catalogs for this driver. Schemas aren't used. Couchbase buckets are
 * equivalent to tables, in that each bucket is a table.
 */
public class CouchbaseMetaData implements DatabaseMetaData {

    private final CouchbaseConnection connection;
    private final CouchbaseJdbcDriver driver;

    CouchbaseMetaData(@NotNull CouchbaseConnection connection, @NotNull CouchbaseJdbcDriver driver) {
        this.connection = connection;
        this.driver = driver;
    }

    @Override
    public ResultSet getSchemas() {
        return null;
    }

    @Override
    public ResultSet getCatalogs() {
        return null;
    }

    public ResultSet getTables(String catalogName, String schemaPattern,
                               String tableNamePattern, String[] types) {
        return null;
    }

    public ResultSet getColumns(String catalogName, String schemaName,
                                String tableNamePattern, String columnNamePattern) {
        return null;
    }

    public ResultSet getPrimaryKeys(String catalogName, String schemaName, String tableNamePattern) {
        return null;
    }

    public ResultSet getIndexInfo(String catalogName, String schemaName, String tableNamePattern, boolean unique,
                                  boolean approximate) {
        return null;
    }

    public ResultSet getTypeInfo() {
        return null;
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
        return "Couchbase";
    }

    public String getDatabaseProductVersion() throws SQLException {
        List<JsonObject> results = connection.getCluster()
                .query("SELECT version() FROM system:dual;")
                .rowsAsObject();
        if (results.size() == 1) {
            JsonObject jsonObject = results.get(0);
            if (jsonObject.containsKey("$1")) {
                return String.valueOf(jsonObject.get("$1"));
            }
        }
        throw new SQLException("Unable to fetch database version");
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
        return null;
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
        //todo
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
        //todo
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
        //todo
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() {
        //todo
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
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalogName, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) {
        return null;
    }

    @Override
    public ResultSet getTableTypes() {
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalogName, String schemaName,
                                         String table, String columnNamePattern) {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalogName, String schemaPattern, String tableNamePattern) {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalogName, String schemaName, String table, int scope,
                                          boolean nullable) {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalogName, String schemaName, String table) {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalogName, String schemaName, String tableNamePattern) {
        return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalogName, String schemaName, String tableNamePattern) {
        return null;
    }


    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) {
        return null;
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
        return null;
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
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalogName, String schemaPattern, String tableNamePattern) {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalogName, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) {
        return null;
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
    public ResultSet getSchemas(String catalogName, String schemaPattern) {
        return null;
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
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getFunctions(String catalogName, String schemaPattern, String functionNamePattern) {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalogName, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalogName, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }
}
