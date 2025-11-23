package id.morantesbryan;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class ImprimirRegistros {
    // ...existing code...

    public static void imprimirRegistros(Connection con) throws SQLException {
        String sql = "SELECT * FROM CLIENTES";
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append("\t");
                    sb.append(md.getColumnLabel(i)).append("=").append(rs.getString(i));
                }
                System.out.println(sb.toString());
            }
        }
    }

    // Nuevo método: usa getInt() para CP y muestra ambos resultados (si existe CP)
    public static void imprimirRegistros2(Connection con) throws SQLException {
        String sql = "SELECT * FROM CLIENTES";
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();

            // buscar índice de la columna CP (case-insensitive)
            int cpIndex = -1;
            for (int i = 1; i <= cols; i++) {
                if ("cp".equalsIgnoreCase(md.getColumnLabel(i))) {
                    cpIndex = i;
                    break;
                }
            }

            while (rs.next()) {
                // imprimir todas las columnas (string)
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append("\t");
                    sb.append(md.getColumnLabel(i)).append("=").append(rs.getString(i));
                }

                // si existe CP, mostrar también getInt()
                if (cpIndex != -1) {
                    int cpInt = rs.getInt(cpIndex);
                    boolean wasNull = rs.wasNull();
                    String cpStr = rs.getString(cpIndex);
                    sb.append("\tCP(as int)=").append(wasNull ? "NULL" : Integer.toString(cpInt));
                    sb.append("\tCP(as str)=").append(cpStr);
                } else {
                    sb.append("\t[Columna 'CP' no encontrada]");
                }

                System.out.println(sb.toString());
            }
        }
    }

    // Método main de prueba
    public static void main(String[] args) throws Exception {
        String url = "jdbc:mysql://localhost:3306/hr_database";
        String user = "root";
        String pass = "4444";

        try (Connection con = DriverManager.getConnection(url, user, pass)) {
            System.out.println("Usando imprimirRegistros (SELECT *):");
            imprimirRegistros(con);
            System.out.println("\nUsando imprimirRegistros2 (getInt vs getString para CP):");
            imprimirRegistros2(con);
        }
    }
    // ...existing code...
}
