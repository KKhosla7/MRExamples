package com.insecure.hive.clients;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Karan Khosla
 */
public class HiveJDBCBasicClient {

    private static final String tableName = "users";
    private static Connection connection;
    private static Statement statement;
    private static ResultSet resultSet;
    private static Map<String, String> hiveQLQueries = new HashMap<String, String>() {{
        put("dropTable", "DROP TABLE IF EXISTS ");
        put("createTableWith", "CREATE TABLE ");
        put("showTable", "SHOW TABLES ");
        put("describeTable", "DESCRIBE ");
        put("loadData", "LOAD DATA LOCAL INPATH ");
        put("queryTable", "SELECT * FROM ");
        put("rowCount", "SELECT COUNT(1) FROM ");
    }};


    public static void main(String[] args) {
        String hiveDriverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
        loadDriver(hiveDriverName);
        try {
            // Drop Table
            runHiveQL(hiveQLQueries.get("dropTable") + tableName);
            closeResources();

            // Create Table
            runHiveQL(hiveQLQueries.get("createTableWith") + tableName + "(id INT, name STRING)");
            closeResources();

            // Show Selected Table
            resultSet = runHiveQL(hiveQLQueries.get("showTable") + "'" + tableName + "'");
            if (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }
            closeResources();

            // Show all Tables
            resultSet = runHiveQL(hiveQLQueries.get("showTable"));
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }
            closeResources();

            // Select All From Table
            resultSet = runHiveQL(hiveQLQueries.get("queryTable") + tableName);
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1) + "\t" + resultSet.getString(2));
            }
            closeResources();

            // Load Data
            String filePath = "input/hive/tables/users.txt";
            resultSet = runHiveQL(hiveQLQueries.get("loadData") + "'" + filePath + "' into table " + tableName);
            closeResources();

            // Select All From Table
            resultSet = runHiveQL(hiveQLQueries.get("queryTable") + tableName);
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1) + "\t" + resultSet.getString(2));
            }
            closeResources();

            // Count
            resultSet = runHiveQL(hiveQLQueries.get("rowCount") + tableName);
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }
            closeResources();

            // Describe Table
            resultSet = runHiveQL(hiveQLQueries.get("describeTable") + tableName);
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1) + "\t" + resultSet.getString(2));
            }
            closeResources();
        } catch (SQLException e) {
            System.out.println("Error executing query.");
            e.printStackTrace();
            System.exit(-1);
        }
    }


    public static ResultSet runHiveQL(String query) throws SQLException {
        Connection hiveServer = connectURI("jdbc:hive://localhost:10000/default", "", "");
        System.out.println("Executing Query: " + query);
        Statement retrievedStatement = retrieveStatement(hiveServer);
        resultSet = retrievedStatement.executeQuery(query);
        return resultSet;
    }

    private static void closeResources() {
        try {
            connection.close();
            statement.close();
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Statement retrieveStatement(Connection hiveServer) {
        try {
            statement = hiveServer.createStatement();
        } catch (SQLException e) {
            System.out.println("Failed to instantiate Statement");
            e.printStackTrace();
            System.exit(-1);
        }
        return statement;
    }

    private static Connection connectURI(String url, String user, String password) {
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            System.out.println("Failed to establish connection");
            e.printStackTrace();
            System.exit(-1);
        }
        return connection;
    }


    public static void loadDriver(String driverName) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found: " + driverName);
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
