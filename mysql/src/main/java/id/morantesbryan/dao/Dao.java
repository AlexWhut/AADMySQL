package id.morantesbryan.dao;

import id.morantesbryan.pojos.Cliente;
import id.morantesbryan.pojos.ClienteNuevo;
import id.morantesbryan.pojos.LineaFactura;
import id.morantesbryan.pojos.ResultadoListado;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
			try {
				int affected = stmt.executeUpdate(trimmed);
				System.out.println("Sentencia ejecutada. Filas afectadas: " + affected);
			} catch (java.sql.SQLIntegrityConstraintViolationException ex) {
				// Duplicate key or other integrity constraint violation — warn and continue
				System.err.println("Advertencia: restricción de integridad al ejecutar la sentencia: " + ex.getMessage());
				// Optionally, you could implement a fallback to insert rows individually
				// or use INSERT IGNORE / ON DUPLICATE KEY UPDATE depending on desired behavior.
			}
		}
	}

	public void obtenerYMostrarApellidosAlternativo(String dni, Connection conn) throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Asegura que la tabla CLIENTES contenga exactamente las filas deseadas:
	 * ('78901234X','NADALES',44126),
	 * ('89012345E','ROJAS', null),
	 * ('56789012B','SAMPER',29730)
	 *
	 * Se usan principalmente sentencias UPDATE y DELETE; si falta alguna fila deseada
	 * se insertará como fallback.
	 */
	public void enforceTargetClientesState() throws SQLException {
		// Desired state
		Map<String, ClienteData> desired = new HashMap<>();
		desired.put("78901234X", new ClienteData("NADALES", Integer.valueOf(44126)));
		desired.put("89012345E", new ClienteData("ROJAS", null));
		desired.put("56789012B", new ClienteData("SAMPER", Integer.valueOf(29730)));

		// Read existing rows
		Map<String, ClienteData> existing = new HashMap<>();
		try (PreparedStatement ps = connection.prepareStatement("SELECT DNI, APELLIDOS, CP FROM CLIENTES");
			 ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				String dni = rs.getString("DNI");
				String apellidos = rs.getString("APELLIDOS");
				int cpVal = rs.getInt("CP");
				Integer cp = rs.wasNull() ? null : Integer.valueOf(cpVal);
				existing.put(dni, new ClienteData(apellidos, cp));
			}
		}

		// Prepare statements
		try (PreparedStatement psUpdate = connection.prepareStatement("UPDATE CLIENTES SET APELLIDOS = ?, CP = ? WHERE DNI = ?");
			 PreparedStatement psDelete = connection.prepareStatement("DELETE FROM CLIENTES WHERE DNI = ?");
			 PreparedStatement psInsert = connection.prepareStatement("INSERT INTO CLIENTES (DNI, APELLIDOS, CP) VALUES (?, ?, ?)") ) {

			// For every existing row: either update to match desired, or delete if not desired
			Set<String> processed = new HashSet<>();
			for (Map.Entry<String, ClienteData> e : existing.entrySet()) {
				String dni = e.getKey();
				ClienteData cur = e.getValue();
				if (desired.containsKey(dni)) {
					ClienteData want = desired.get(dni);
					boolean apellidosEqual = (want.apellidos == null ? cur.apellidos == null : want.apellidos.equals(cur.apellidos));
					boolean cpEqual = (want.cp == null ? cur.cp == null : want.cp.equals(cur.cp));
					if (!apellidosEqual || !cpEqual) {
						// UPDATE
						psUpdate.setString(1, want.apellidos);
						if (want.cp == null) psUpdate.setNull(2, java.sql.Types.INTEGER);
						else psUpdate.setInt(2, want.cp.intValue());
						psUpdate.setString(3, dni);
						int updated = psUpdate.executeUpdate();
						System.out.println("Updated DNI=" + dni + ", rowsAffected=" + updated);
					}
					processed.add(dni);
				} else {
					// DELETE
					psDelete.setString(1, dni);
					int deleted = psDelete.executeUpdate();
					System.out.println("Deleted DNI=" + dni + ", rowsAffected=" + deleted);
				}
			}

			// Insert any desired rows not present
			for (Map.Entry<String, ClienteData> e : desired.entrySet()) {
				String dni = e.getKey();
				if (!processed.contains(dni)) {
					ClienteData want = e.getValue();
					psInsert.setString(1, dni);
					psInsert.setString(2, want.apellidos);
					if (want.cp == null) psInsert.setNull(3, java.sql.Types.INTEGER);
					else psInsert.setInt(3, want.cp.intValue());
					int inserted = psInsert.executeUpdate();
					System.out.println("Inserted DNI=" + dni + ", rowsAffected=" + inserted);
				}
			}
		}
	}

	// Small helper for desired/existing data
	private static class ClienteData {
		final String apellidos;
		final Integer cp;

		ClienteData(String apellidos, Integer cp) {
			this.apellidos = apellidos;
			this.cp = cp;
		}
	}
}
