package com.dbschema;

import com.couchbase.client.java.json.JsonObject;

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

    CouchbaseMetaData(CouchbaseConnection connection, CouchbaseJdbcDriver driver) {
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
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean allProceduresAreCallable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean allTablesAreSelectable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getURL() {
        return null;
    }

    public String getUserName() {
        return null;
    }

    public boolean isReadOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullsAreSortedLow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new SQLFeatureNotSupportedException();
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

    public boolean usesLocalFiles() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() {
        return true;
    }

    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    public boolean storesUpperCaseQuotedIdentifiers() {
        return true;
    }

    public boolean storesLowerCaseQuotedIdentifiers() {
        return true;
    }

    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
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

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsColumnAliasing() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsConvert() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsGroupBy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsNonNullableColumns() {
        return true;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOuterJoins() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getSchemaTerm() {
        return "namespace";
    }

    public String getProcedureTerm() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getCatalogTerm() {
        return "namespace";
    }

    public boolean isCatalogAtStart() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getCatalogSeparator() {
        return ":";
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsPositionedDelete() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsStoredProcedures() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsUnion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsUnionAll() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxCharLiteralLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInIndex() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInSelect() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxColumnsInTable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxConnections() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxCursorNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxIndexLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxSchemaNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxProcedureNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxCatalogNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxRowSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxStatementLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxStatements() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxTableNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getMaxTablesInSelect() {
        return 1;
    }

    public int getMaxUserNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    public boolean supportsTransactions() {
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsBatchUpdates() {
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
    public boolean supportsNamedParameters() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getSchemas(String catalogName, String schemaPattern) {
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
    public ResultSet getPseudoColumns(String catalogName, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
