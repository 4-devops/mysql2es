package com.zlinfo.platform.mysql2es.config;

public class MysqlConf {
    private String driver;
    private String url;
    private String username;
    private String password;
    private String sql;
    private int recordId;
    private String id;
    private String clusterName;
    private String host;
    private int port;
    private String indexTable;
    private String updateById;
    private String updateByTime;
    private int bulkSize;

    public int getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    public String getUpdateById() {
        return updateById;
    }

    public void setUpdateById(String updateById) {
        this.updateById = updateById;
    }

    public String getUpdateByTime() {
        return updateByTime;
    }

    public void setUpdateByTime(String updateByTime) {
        this.updateByTime = updateByTime;
    }

    public String getIndexTable() {
        return indexTable;
    }

    public void setIndexTable(String indexTable) {
        this.indexTable = indexTable;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

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
