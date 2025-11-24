package com.acc.datos.hibernate_project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcTest {
    public static void main(String[] args) {
        String url = "jdbc:mysql://127.0.0.1:3306/proyecto_orm?serverTimezone=UTC";
        String user = "libro_ad";
        String pass = "libro1234";
        System.out.println("Testing JDBC connection to: " + url);
        try (Connection c = DriverManager.getConnection(url, user, pass)) {
            System.out.println("Connection successful: autoCommit=" + c.getAutoCommit());
        } catch (SQLException e) {
            System.err.println("Connection failed:");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
