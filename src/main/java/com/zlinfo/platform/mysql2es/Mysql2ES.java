package com.zlinfo.platform.mysql2es;

import com.zlinfo.platform.mysql2es.config.MysqlConf;
import com.zlinfo.platform.mysql2es.utils.MixAllUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Properties;

/**
 * Created by zhulinfeng on 2018/9/26.
 */
public class Mysql2ES {

    private static final Logger LOGGER = LoggerFactory.getLogger(Mysql2ES.class);

    public static void main(String[] args) {
        Properties properties = MixAllUtils.file2Properties("application.properties");
        MysqlConf conf = new MysqlConf();
        MixAllUtils.properties2Object(properties, conf);
        String driver = conf.getDriver();
        String url = conf.getUrl();
        String user = conf.getUsername();
        String passwd = conf.getPassword();
        String sql = conf.getSql();

        TransportClient client = ES.getSingleClient(conf);

        LOGGER.info("the configuration of this application is : \n\t" +
                "Driver : " + driver + "\n\t" +
                "Url : " + url + "\n\t" +
                "User : " + user + "\n\t" +
                "Password : " + passwd + "\n\t" +
                "SQL : " + sql);
        Connection connection = Mysql.getConnection(driver, url, user, passwd);

        while (true) {
            try {
                IncrementalUpdateById.start(connection, conf, sql, client);
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
