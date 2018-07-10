package com.fluxedo.es.descriptors;

/**
 * Created by Marco Balduini on 03/07/2018 as part of project esperservices.
 */
public class JDBCConnectionDescriptor {

    private String connectionName;
    private String driver;
    private String jdbcConnectionString;
    private String username;
    private String password;

    public JDBCConnectionDescriptor() {
    }

    public JDBCConnectionDescriptor(String connectionName, String driver, String jdbcConnectionString, String username, String password) {
        this.connectionName = connectionName;
        this.driver = driver;
        this.jdbcConnectionString = jdbcConnectionString;
        this.username = username;
        this.password = password;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getJdbcConnectionString() {
        return jdbcConnectionString;
    }

    public void setJdbcConnectionString(String jdbcConnectionString) {
        this.jdbcConnectionString = jdbcConnectionString;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
