package io.crate.client.jdbc;

import io.crate.action.sql.SQLRequest;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class CratePreparedStatement extends CrateStatement implements PreparedStatement {

    private final SQLRequest sqlRequest = new SQLRequest();
    private final Map<Integer, Object> arguments = new TreeMap<>();

    public CratePreparedStatement(CrateConnection connection, String stmt) {
        super(connection);
        sqlRequest.stmt(stmt);
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
        throw new SQLFeatureNotSupportedException("PreparedStatement: executeUpdate() not supported");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        sqlRequest.stmt(sql);
        execute();
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        arguments.put(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: setBytes() not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        arguments.put(parameterIndex, x.getTime());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        arguments.put(parameterIndex, x.getTime());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        arguments.put(parameterIndex, x.getTime());
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
        arguments.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        arguments.put(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        sqlRequest.args(arguments.values().toArray(new Object[arguments.size()]));
        sqlResponse = connection.client().sql(sqlRequest).actionGet();
        if (sqlResponse.rowCount() <= 0) {
            return false;
        }
        resultSet = new CrateResultSet(this, sqlResponse);
        return true;
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement: addBatch() not supported");
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
        arguments.put(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        //return null;
        return new CrateResultSetMetaData(new ArrayList<String>(2){{add("id");add("name");}});
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
