package com.zlinfo.platform.mysql2es.config;

public class MysqlConf {
    private String driver = null;
    private String url = null;
    private String username = null;
    private String password = null;
    private String sql = null;

    public String getDriver() {
        return driver;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getSql() {
        return sql;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
