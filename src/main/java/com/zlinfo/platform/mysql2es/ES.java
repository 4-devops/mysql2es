package com.zlinfo.platform.mysql2es;

import com.zlinfo.platform.mysql2es.config.MysqlConf;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class ES {

    public static volatile TransportClient client;

    public static TransportClient getSingleClient(MysqlConf conf) {

        if (client == null) {
            Settings settings = Settings.builder()
                    .put("cluster.name", conf.getClusterName())
                    .put("client.transport.sniff", true)
                    .build();
            synchronized (TransportClient.class) {
                try {
                    client = new PreBuiltTransportClient(settings)
                            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(conf.getHost()),
                                    conf.getPort()));
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }
        }
        return client;
    }

    public static void createIndex(TransportClient client, String index,
                                   String type, Map<String, String> schemaMap) {
        CreateIndexRequestBuilder requestBuilder = client.admin()
                .indices().prepareCreate(index);
        for (String key : schemaMap.keySet()) {
            String fieldType = "keyword";
            String tmpType = schemaMap.get(key);
            if (tmpType.equals("INT")) {
                fieldType = "integer";
            } else if (tmpType.equals("DATE")) {
                fieldType = "date";
            }
            schemaMap.put(key, fieldType);
        }
    }
}
