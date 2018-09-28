package com.zlinfo.platform.mysql2es;

import com.zlinfo.platform.mysql2es.config.MysqlConf;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class IncrementalUpdateByTime {
    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalUpdateByTime.class);

    public static void start(Connection connection, MysqlConf conf, String sql) {
        String lastIdSQL = "select es_db_name, es_table_name, last_update_time from " + conf.getIndexTable() + " where id=" + conf.getRecordId();
        //System.out.println(lastIdSQL);
        //System.exit(1);
        List<Map<String, Object>> record = Mysql.getData(connection,lastIdSQL);
        int last_update_time = Integer.parseInt(String.valueOf(record.get(0).get("last_update_time")));
        String esIndex = String.valueOf(record.get(0).get("es_db_name"));
        String esType = String.valueOf(record.get(0).get("es_table_name"));

        sql.replaceAll("\\$\\{field\\}", conf.getUpdateById());
        sql.replaceAll("\\$\\{condition\\}", String.valueOf(last_update_time));


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
                        LOGGER.error("fail to bulk data by time");
                    } else {
                        LOGGER.info("bulk data by time success once");
                    }
                    tag = 0;
                    bulkRequest = null;
                }
            }

            if (tag > 0) {
                BulkResponse bulkResponse = bulkRequest.get();
                if (bulkResponse.hasFailures()) {
                    LOGGER.error("fail to bulk data by time");
                } else {
                    LOGGER.info("bulk data by time is success and finish in this query, update mysql");
                    int tmp_time = Integer.parseInt(String.valueOf(resultList.get(total - 1)
                            .get(conf.getUpdateByTime())));
                    String tmp = "update " + conf.getIndexTable() + "set last_id=" + tmp_time +
                            "where id=" + conf.getRecordId();
                    Mysql.update(connection, tmp);
                }
            }
        } else {
            LOGGER.info("no data to update by last_update_time");
        }
    }
}
