package com.zlinfo.platform.mysql2es;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhulinfeng on 2018/9/26.
 */
public class Mysql {

    public static Connection getConnection(String driver, String url, String user, String passwd) {
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, passwd);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static List<Map<String, Object>> getData (Connection conn, String sql) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        List<Map<String, Object>> list = new ArrayList<>();
        //List<String> typeList = new ArrayList<>();
        try {
            statement = conn.prepareStatement(sql);
            resultSet = statement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();

            int col_nums = metaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < col_nums; i++) {
                    String col_name = metaData.getColumnName(i + 1);
                    Object col_value = resultSet.getObject(col_name);
                    map.put(col_name, col_value);
                }
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                resultSet.close();
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static void update(Connection connection, String sql) {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(sql);
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
