package com.intellij.executor;

import java.sql.ResultSet;

public class ExecutionResult {
    private final boolean isSuccess;
    private final ResultSet resultSet;
    private final int updateCount;

    public ExecutionResult(boolean isSuccess) {
        this(isSuccess, null, -1);
    }

    public ExecutionResult(boolean isSuccess, ResultSet resultSet) {
        this(isSuccess, resultSet, -1);
    }

    public ExecutionResult(boolean isSuccess, ResultSet resultSet, int updateCount) {
        this.isSuccess = isSuccess;
        this.resultSet = resultSet;
        this.updateCount = updateCount;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public int getUpdateCount() {
        return updateCount;
    }
}
