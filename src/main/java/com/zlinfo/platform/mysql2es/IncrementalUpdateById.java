package com.zlinfo.platform.mysql2es;

import com.sun.deploy.util.StringUtils;
import com.zlinfo.platform.mysql2es.config.MysqlConf;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class IncrementalUpdateById {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalUpdateById.class);

    public static void start(Connection connection, MysqlConf conf, String sql) {
        String lastIdSQL = "select es_db_name, es_table_name, last_id from " + conf.getIndexTable() + " where id=" + conf.getRecordId();
        //System.out.println(lastIdSQL);
        //System.exit(1);
        List<Map<String, Object>> record = Mysql.getData(connection,lastIdSQL);
        long last_id = Long.parseLong(String.valueOf(record.get(0).get("last_id")));
        String esIndex = String.valueOf(record.get(0).get("es_db_name"));
        String esType = String.valueOf(record.get(0).get("es_table_name"));

        String[] arr = sql.split(" ");
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].equals("${field}")) {
                arr[i] = conf.getUpdateById();
            }

            if (arr[i].equals("${condition}")) {
                arr[i] = String.valueOf(last_id);
            }
        }

        sql = StringUtils.join(arr," ");
        sql = 

        System.out.println(sql);
        System.exit(1);
        List<Map<String, Object>> resultList = Mysql.getData(connection, sql);

        if (resultList.size() > 0) {
            TransportClient client = ES.getSingleClient(conf);
            int tag = 0;
            int total = resultList.size();
            BulkRequestBuilder bulkRequest = client.prepareBulk();

            for (int index = 0; index < total; index++) {
                if (tag < conf.getBulkSize()) {
                    bulkRequest.add(client.prepareIndex(esIndex, esType,
                            String.valueOf(resultList.get(index).get(conf.getId())))
                            .setSource(resultList.get(index)));
                    tag++;
                } else if (tag >= conf.getBulkSize()) {
                    BulkResponse bulkResponse = bulkRequest.get();
                    if (bulkResponse.hasFailures()) {
                        LOGGER.error("fail to bulk data by id");
                    } else {
                        LOGGER.info("bulk once success");
                    }
                    tag = 0;
                    bulkRequest = null;
                }
            }

            if (tag > 0) {
                BulkResponse bulkResponse = bulkRequest.get();
                if (bulkResponse.hasFailures()) {
                    LOGGER.error("fail to bulk data by id");
                } else {
                    LOGGER.info("bulk success and finish in this query, update mysql");
                    long tmp_id = Long.parseLong(String.valueOf(resultList.get(total)
                            .get(conf.getUpdateById())));
                    String tmp = "update " + conf.getIndexTable() + "set last_id=" + tmp_id +
                            "where id=" + conf.getRecordId();
                    Mysql.update(connection, tmp);
                }
            }
        } else {
            LOGGER.info("no data to update by last_id");
        }
    }
}
