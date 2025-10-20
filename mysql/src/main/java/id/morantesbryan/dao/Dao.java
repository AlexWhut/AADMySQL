package id.morantesbryan.dao;

import id.morantesbryan.pojos.Cliente;
import id.morantesbryan.pojos.ClienteNuevo;
import id.morantesbryan.pojos.LineaFactura;
import id.morantesbryan.pojos.ResultadoListado;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class Dao {

	private final Connection connection;

	public Dao(Connection connection) {
		this.connection = connection;
	}

	// Stub: insertar clientes en lote. Returns array of update counts.
	public int[] insertarClientesEnLote(Connection conn, List<Cliente> clientes) throws SQLException {
		// Minimal implementation: pretend each insert succeeded with 1
		int[] results = new int[clientes.size()];
		for (int i = 0; i < results.length; i++) results[i] = 1;
		return results;
	}

	// Other methods used in Main: provide stubs or minimal implementations
	public void insertarClientes(Connection conn, List<Cliente> clientes) throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

	public void insertarClientesBatchConTransaccion(Connection conn, List<Cliente> clientes) throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

	public Map<String, Integer> crearFacturas(Connection conn, List<String> dnis, List<LineaFactura> lineas) throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

	public ResultadoListado llamarListadoClientes(Connection conn, String dni) throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

	public void modificarClientesConResultSet(Connection conn, String nuevoCp, ClienteNuevo nuevoCliente) throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

	public void crearTablaClientesSiNoExiste() throws SQLException {
		String sql = "CREATE TABLE IF NOT EXISTS CLIENTES (" +
				"DNI VARCHAR(20) PRIMARY KEY, " +
				"APELLIDOS VARCHAR(255), " +
				"CP INT" +
				")";

		try (java.sql.Statement stmt = connection.createStatement()) {
			stmt.execute(sql);
		}
	}

	public void insertarDatosConStatement(Connection conn, String sql) throws SQLException {
		if (sql == null || sql.trim().isEmpty()) {
			throw new IllegalArgumentException("SQL string is null or empty");
		}

		// Remove trailing semicolon if present to avoid driver errors
		String trimmed = sql.trim();
		if (trimmed.endsWith(";")) {
			trimmed = trimmed.substring(0, trimmed.length() - 1);
		}

		try (java.sql.Statement stmt = conn.createStatement()) {
			int affected = stmt.executeUpdate(trimmed);
			System.out.println("Sentencia ejecutada. Filas afectadas: " + affected);
		}
	}

	public void obtenerYMostrarApellidosAlternativo(String dni, Connection conn) throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}
}
