package com.zlinfo.platform.mysql2es;

import com.zlinfo.platform.mysql2es.config.MysqlConf;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Map;


public class IncrementalUpdateById {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalUpdateById.class);

    public static void start(Connection connection, MysqlConf conf, String sql, TransportClient client) {
        String lastIdSQL = "select es_db_name, es_table_name, last_id, db_table_name from " + conf.getIndexTable() + " where id=" + conf.getRecordId();
        List<Map<String, Object>> record = Mysql.getData(connection,lastIdSQL);
        long last_id = Long.parseLong(String.valueOf(record.get(0).get("last_id")));
        String esIndex = String.valueOf(record.get(0).get("es_db_name"));
        String esType = String.valueOf(record.get(0).get("es_table_name"));
        String dbName = String.valueOf(record.get(0).get("db_table_name"));

        StringBuffer sb = new StringBuffer();
        String[] arr = sql.split(" ");
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].equals("${field}")) {
                sb.append(conf.getUpdateById() + " ");
            } else if (arr[i].equals("${condition}")) {
                sb.append(String.valueOf(last_id) + " ");
            } else {
                sb.append(arr[i] + " ");
            }
        }
        String idSql = sb.toString();
        LOGGER.debug(idSql);

        List<Map<String, Object>> resultList = Mysql.getData(connection, idSql);
        Map<String, String> typeMap = Mysql.getMapping(connection,dbName);

        /**
         * 判断索引是否存在，不存在则创建
         */
        IndicesAdminClient adminClient = client.admin().indices();
        IndicesExistsResponse existsResponse = adminClient.prepareExists(esIndex).get();
        if (!existsResponse.isExists()) {
            LOGGER.error("index {} is not exit", esIndex);
            System.exit(1);
            //ES.createIndex(client, esIndex, esType, Mysql.getMapping(connection, dbName));
        }

        if (resultList.size() > 0) {
            int tag = 0;
            int total = resultList.size();
            BulkRequestBuilder bulkRequest = client.prepareBulk();

            for (int index = 1; index <= total; index++) {
                if (tag < conf.getBulkSize()) {
                    Map<String, Object> tmp = resultList.get(index - 1);
                    //System.out.println(typeMap);
                    for (String key : typeMap.keySet()) {
                        String tmpType = typeMap.get(key);
                        if (tmpType.equalsIgnoreCase("int")) {
                            tmp.put(key,Integer.parseInt(String.valueOf(tmp.get(key))));
                        } else if (tmpType.equalsIgnoreCase("bigint")) {
                            tmp.put(key, Long.parseLong(String.valueOf(tmp.get(key))));
                        } else if (tmpType.equalsIgnoreCase("int unsigned")) {
                            tmp.put(key, Integer.parseInt(String.valueOf(tmp.get(key))));
                        } else {
                            tmp.put(key, String.valueOf(tmp.get(key)));
                        }
                    }

                    bulkRequest.add(client.prepareIndex(esIndex, esType,
                            String.valueOf(resultList.get(index - 1).get(conf.getId())))
                            .setSource(tmp));
                    tag++;
                } else if (tag >= conf.getBulkSize()) {
                    BulkResponse bulkResponse = bulkRequest.get();
                    System.out.println(bulkRequest);
                    if (bulkResponse.hasFailures()) {
                        LOGGER.error("fail to bulk data by id");
                    } else {
                        LOGGER.debug("bulk once success");
                    }
                    bulkRequest = client.prepareBulk();
                    Map<String, Object> tmp = resultList.get(index - 1);
                    for (String key : typeMap.keySet()) {
                        String tmpType = typeMap.get(key);
                        if (tmpType.equalsIgnoreCase("int")) {
                            tmp.put(key,Integer.parseInt(String.valueOf(tmp.get(key))));
                        } else if (tmpType.equalsIgnoreCase("char")
                                || tmpType.equalsIgnoreCase("date")
                                || tmpType.equalsIgnoreCase("varchar")) {
                            tmp.put(key, String.valueOf(tmp.get(key)));
                        }
                    }
                    bulkRequest.add(client.prepareIndex(esIndex, esType,
                            String.valueOf(resultList.get(index - 1).get(conf.getId())))
                            .setSource(tmp));
                    tag = 1;
                }
            }

            if (tag >= 1) {
                BulkResponse bulkResponse = bulkRequest.get();
                if (bulkResponse.hasFailures()) {
                    LOGGER.error("fail to bulk data by id");
                } else {
                    LOGGER.info("bulk success and finish in this query, update mysql");
                    long tmp_id = Long.parseLong(String.valueOf(resultList.get(total - 1)
                            .get(conf.getUpdateById())));
                    String tmp = "update " + conf.getIndexTable() + " set last_id=" + tmp_id +
                            " where id=" + conf.getRecordId();
                    LOGGER.debug(tmp);
                    Mysql.update(connection, tmp);
                }
            }
        } else {
            LOGGER.debug("no data to update by last_id");
            IncrementalUpdateByTime.start(connection, conf, sql, client);
        }
    }
}
