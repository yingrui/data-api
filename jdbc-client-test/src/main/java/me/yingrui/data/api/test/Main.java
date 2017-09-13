package me.yingrui.data.api.test;

import java.sql.*;
import java.util.Properties;

/**
 * Created by yingrui on 12/09/2017.
 */
public class Main {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:avatica:remote:url=http://localhost:8080/";
        Properties connectionProperties = new Properties();

        Connection connection = DriverManager.getConnection(url, connectionProperties);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM ITEMS");
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                Object object = resultSet.getObject(i);
                System.out.println(object);
            }
        }
        statement.close();
        connection.close();
    }
}
