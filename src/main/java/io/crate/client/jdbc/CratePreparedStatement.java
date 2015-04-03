package io.crate.client.jdbc;

import io.crate.action.sql.*;
import io.crate.shade.com.google.common.base.Throwables;
import io.crate.shade.org.elasticsearch.common.Nullable;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class CratePreparedStatement extends CrateStatementBase implements PreparedStatement {

    static class CratePreparedStatementParser {
        /**
         * Parses the number of parameters from the given SQL statement.
         * Is aware of '?' and '$1' kind of parameters
         * and does not consider those when occuring in strings.
         *
         * @param statement the SQL statement to get the number of parameters from
         * @return a BitSet with all the parameter slots set ($1 -> "1", (?, ?) -> "11")
         */
        public static BitSet getParameters(String statement) {
            BitSet paramSlots = new BitSet();
            int count = 0;
            boolean insideString = false;

            for (int i = 0; i < statement.length(); i++) {
                switch (statement.charAt(i)) {
                    case '\'':
                        insideString ^= true;
                        break;
                    case '$':
                        if (!insideString) {
                            if (statement.length() > i+1) {
                                // numeric parameter
                                int paramNum = getParamNumber(statement, i+1);
                                if (paramNum > 0) {
                                    paramSlots.set(paramNum-1);
                                }
                            }
                        }
                        break;
                    case '?':
                        if (!insideString) {
                            paramSlots.set(count++);
                        }
                        break;
                    default:

                        break;
                }
            }
            return paramSlots;
        }

        private static int getParamNumber(String statement, int pos) {
            StringBuilder builder = new StringBuilder();
            int i = pos,
                length = statement.length();
            char c;
            while (i < length) {
                c = statement.charAt(i++);
                if ('0' <= c && c <= '9') {
                    builder.append(c);
                } else {
                    break;
                }
            }
            try {
                return Integer.valueOf(builder.toString());
            } catch (NumberFormatException e) {
                return -1;
            }
        }
    }

    private static final int[] BATCH_FAILED_RESPONSE = new int[]{EXECUTE_FAILED};

    private final SQLRequest sqlRequest = new SQLRequest();
    private SQLResponse sqlResponse;

    private final BitSet parameterSlots;
    private BitSet paramsAdded;

    private List<Object[]> batchParams = new LinkedList<>();
    private Object[] currentParams;

    public CratePreparedStatement(CrateConnection connection, String stmt) {
        super(connection);
        try {
            sqlRequest.setDefaultSchema(connection.getSchema());
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
        sqlRequest.stmt(stmt);
        sqlRequest.includeTypesOnResponse(true);
        parameterSlots = CratePreparedStatementParser.getParameters(sqlRequest.stmt());
        paramsAdded = new BitSet(parameterSlots.size());
        currentParams = new Object[parameterSlots.length()];
    }

    protected void checkAllArgumentsProvided() throws SQLException {
        if (!parameterSlots.equals(paramsAdded)) {
            throw new SQLException("Not all parameters have been provided a value");
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        execute();
        return resultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        if (execute()) {
            resultSet = null;
            throw new SQLException("Execution of statement returned a ResultSet");
        } else {
            // return 0 if no affected Rows are given
            return (int)Math.max(0L, sqlResponse.rowCount());
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("executeQuery(String) not supported on PreparedStatement. Use executeQuery().");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("executeUpdate(String) not supported on PreparedStatement. Use executeQuery().");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("execute(String) not supported on PreparedStatement. Use eceute().");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        if (resultSet == null && sqlResponse != null) {
            return (int) sqlResponse.rowCount();
        }
        return -1;
    }

    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        checkAllArgumentsProvided();

        sqlRequest.args(currentParams);
        executeSingle();
        if (!hasResultSet(sqlResponse)) {
            return false;
        }
        resultSet = new CrateResultSet(this, sqlResponse);
        return true;
    }

    private void executeSingle() throws SQLException {
        try {
            sqlResponse = connection.client().sql(sqlRequest).actionGet();
        } catch (SQLActionException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    private boolean hasResultSet(SQLResponse response) {
        return response.rowCount() > 0 && response.rowCount() == response.rows().length;
    }

    private void set(int idx, @Nullable Object value) throws SQLException {
        checkClosed();
        try {
            currentParams[idx-1] = value;
            paramsAdded.set(idx-1);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException(
                    String.format(Locale.ENGLISH, "invalid parameter index %d", idx));
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        set(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setBytes() not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        set(parameterIndex, x.getTime());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        set(parameterIndex, x.getTime());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        set(parameterIndex, x.getTime());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setAsciiStream() not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setUnicodeStream() not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setBinaryStream() not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        paramsAdded.clear();
        Arrays.fill(currentParams, null);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        set(parameterIndex, x);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLException("cannot call addBatch(String) on PreparedStatement.");
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        checkAllArgumentsProvided();
        batchParams.add(currentParams);
        currentParams = new Object[parameterSlots.length()];
        clearParameters(); // init new params and reset counter
    }

    @Override
    public void clearBatch() throws SQLException {
        batchParams.clear();
    }


    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        int[] results;
        DatabaseMetaData metaData = connection.getMetaData();
        if (VersionStringComparator.compareVersions(
                metaData.getDatabaseProductVersion(),
                CrateDatabaseMetaData.CRATE_BULK_ARG_VERSION) >= 0) {
            results = executeBatchBulk();
        } else {
            results = executeBatchSingle();
        }
        clearBatch();
        return results;

    }

    private int[] executeBulk(SQLBulkRequest bulkRequest) throws SQLException {
        SQLBulkResponse bulkResponse;
        try {
            bulkResponse = connection.client().bulkSql(bulkRequest).actionGet();
        } catch (SQLActionException e) {
            throw new SQLException(e.getMessage(), e);
        }
        int[] results = new int[bulkResponse.results().length];
        SQLBulkResponse.Result[] results1 = bulkResponse.results();
        for (int i = 0, results1Length = results1.length; i < results1Length; i++) {
            SQLBulkResponse.Result result = results1[i];
            if (result.errorMessage() != null || result.rowCount() == -2) {
                results[i] = EXECUTE_FAILED;
            } else {
                results[i] = (result.rowCount() >= 0 ? (int)result.rowCount() : SUCCESS_NO_INFO);
            }
        }
        return results;
    }

    private int[] executeBatchBulk() throws SQLException {
        SQLBulkRequest bulkRequest = new SQLBulkRequest(sqlRequest.stmt(),
                batchParams.toArray(new Object[batchParams.size()][]));
        bulkRequest.includeTypesOnResponse(true);
        try {
            return executeBulk(bulkRequest);
        } catch (SQLException e) {
            // we cannot know what batch worked and what went wrong here
            throw new BatchUpdateException(e.getMessage(), BATCH_FAILED_RESPONSE, e);
        }
    }

    private int[] executeBatchSingle() throws SQLException {
        int[] results = new int[batchParams.size()];
        boolean failed = false;
        for (int i = 0, batchParamsSize = batchParams.size(); i < batchParamsSize; i++) {
            Object[] params = batchParams.get(i);
            sqlRequest.args(params);
            try {
                executeSingle();
                results[i] = (int) sqlResponse.rowCount();
            } catch (SQLException e) {
                results[i] = EXECUTE_FAILED;
                failed = true;
            }
        }
        if (failed) {
            throw new BatchUpdateException("Error during batch update", results);
        }
        return results;
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setCharacterStream() not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setRef() not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setBlob() not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setClob() not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        set(parameterIndex, x.getArray());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        if (resultSet != null) {
            return resultSet.getMetaData();
        } else {
            return null;
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setString(parameterIndex, x.toString());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setRowId() not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setNString() not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setClob() not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setNClob() not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setClob(reader) not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setBlob(inputStream) not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setNClob(reader) not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setSQLXML() not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setAsciiStream(inputStream) not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setBinaryStream(inputStream) not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setCharacterStream(reader) not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setAsciiStream(inputStream) not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setBinaryStream(inputStream) not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setCharacterStream(reader) not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setNCharacterStream(reader) not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setClob(reader) not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setBlob(inputStream) not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setNClob(reader) not supported");
    }
}
