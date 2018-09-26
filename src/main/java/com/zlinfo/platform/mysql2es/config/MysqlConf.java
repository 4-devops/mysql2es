package com.zlinfo.platform.mysql2es.config;

public class MysqlConf {
    private String driver;
    private String url;
    private String username;
    private String password;
    private String sql;
    private int recordId;
    private String id;

    public int getRecordId() {
        return recordId;
    }

    public String getId() {
        return id;
    }

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

    public void setRecordId(int recordId) {
        this.recordId = recordId;
    }

    public void setId(String id) {
        this.id = id;
    }
}
