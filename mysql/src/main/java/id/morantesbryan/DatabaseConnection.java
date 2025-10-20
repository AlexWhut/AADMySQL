package id.morantesbryan;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public final class DatabaseConnection {

    private static final String CONFIG_FILE = "config.properties";

    private DatabaseConnection() { /* util class */ }

    // Obtiene una conexión nueva usando config.properties (db.url, db.user, db.password, db.driver)
    public static Connection getConnection() throws SQLException {
        Properties props = loadProperties();

        String url = props.getProperty("db.url");
        String user = props.getProperty("db.user");
        String password = props.getProperty("db.password");
        String driver = props.getProperty("db.driver");

        if (driver != null && !driver.isBlank()) {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Driver JDBC no encontrado: " + driver, e);
            }
        }

        return DriverManager.getConnection(url, user, password);
    }

    // Carga properties desde resources/config.properties
    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream in = Thread.currentThread()
                                     .getContextClassLoader()
                                     .getResourceAsStream(CONFIG_FILE)) {
            if (in == null) {
                throw new RuntimeException("No se encontró " + CONFIG_FILE + " en resources.");
            }
            props.load(in);
            return props;
        } catch (IOException e) {
            throw new RuntimeException("Error al leer " + CONFIG_FILE, e);
        }
    }

    // Cierra la conexión sin lanzar excepción
    public static void closeQuietly(Connection conn) {
        if (conn == null) return;
        try {
            if (!conn.isClosed()) conn.close();
        } catch (SQLException ignored) { }
    }
}