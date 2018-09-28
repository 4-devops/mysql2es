package com.zlinfo.platform.mysql2es;

import com.zlinfo.platform.mysql2es.config.MysqlConf;
import com.zlinfo.platform.mysql2es.utils.MixAllUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

        LOGGER.info("mysql config is :");
        LOGGER.info("\t\tDriver : " + driver + "\n\t\tURL : " + url + "\n\t\tUSER : "
                + user + "\n\t\tPASSWORD : " + passwd + "\n\t\tSQL : " + sql);
        Connection connection = Mysql.getConnection(driver, url, user, passwd);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                IncrementalUpdateById.start(connection, conf, sql);
                IncrementalUpdateByTime.start(connection, conf, sql);
            }
        };

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(runnable, 10, 2, TimeUnit.SECONDS);
    }

}
