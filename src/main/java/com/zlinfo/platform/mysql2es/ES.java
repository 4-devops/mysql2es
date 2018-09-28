package com.zlinfo.platform.mysql2es;

import com.zlinfo.platform.mysql2es.config.MysqlConf;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
                            .addTransportAddress(new TransportAddress(InetAddress.getByName(conf.getHost()),
                                    conf.getPort()));
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }
        }
        return client;
    }
}
