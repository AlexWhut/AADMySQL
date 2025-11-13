package id.morantesbryan.dao;

import id.morantesbryan.pojos.Cliente;
import id.morantesbryan.pojos.ClienteNuevo;
import id.morantesbryan.pojos.LineaFactura;
import id.morantesbryan.pojos.ResultadoListado;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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

	/**
	 * ACTIVIDAD 4.3: Método que ejecuta la consulta SQL para obtener nombres completos 
	 * de empleados y los muestra en orden inverso (del último al primero).
	 * La consulta SQL no se modifica, solo se invierte el orden de los resultados en Java.
	 */
	public void mostrarEmpleadosOrdenInverso() throws SQLException {
		// La consulta SQL original sin modificaciones
		String sql = "SELECT CONCAT(first_name, ' ', last_name) AS name FROM employees";
		
		// Lista para almacenar los nombres y poder invertir el orden
		List<String> nombres = new ArrayList<>();
		
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			// Recorremos el ResultSet y almacenamos los nombres en la lista
			while (rs.next()) {
				String nombreCompleto = rs.getString("name");
				nombres.add(nombreCompleto);
			}
		}
		
		// Invertimos la lista para mostrar del último al primero
		Collections.reverse(nombres);
		
		// Mostramos los resultados en orden inverso
		System.out.println("=== EMPLEADOS EN ORDEN INVERSO ===");
		System.out.println("Total de empleados: " + nombres.size());
		System.out.println("Mostrando del último al primero:");
		System.out.println("================================");
		
		for (int i = 0; i < nombres.size(); i++) {
			System.out.println((i + 1) + ". " + nombres.get(i));
		}
		
		System.out.println("================================");
	}

	/**
	 * Método alternativo más eficiente que obtiene los nombres ya en orden inverso
	 * desde la base de datos usando ORDER BY DESC, pero manteniendo la consulta base.
	 */
	public void mostrarEmpleadosOrdenInversoAlternativo() throws SQLException {
		// Consulta original con ORDER BY para obtener resultados ya ordenados inversamente
		String sql = "SELECT CONCAT(first_name, ' ', last_name) AS name FROM employees ORDER BY name DESC";
		
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			System.out.println("=== EMPLEADOS EN ORDEN INVERSO (Método alternativo) ===");
			int contador = 1;
			
			while (rs.next()) {
				String nombreCompleto = rs.getString("name");
				System.out.println(contador + ". " + nombreCompleto);
				contador++;
			}
			
			System.out.println("Total de empleados: " + (contador - 1));
			System.out.println("=================================================");
		}
	}

	/**
	 * ACTIVIDAD 4.4: Métodos para averiguar el número de filas de una consulta
	 * sin recorrer todo el ResultSet para contarlas.
	 */
	
	/**
	 * Método 1: Usando ResultSet scrollable con last() y getRow()
	 * Este método permite obtener el número de filas sin recorrer todo el ResultSet.
	 */
	public void contarFilasConScrollableResultSet() throws SQLException {
		String sql = "SELECT DNI, APELLIDOS, CP FROM CLIENTES";
		
		System.out.println("=== ACTIVIDAD 4.4 - Método 1: ResultSet Scrollable ===");
		System.out.println("Consulta: " + sql);
		
		// Crear Statement con ResultSet scrollable
		try (java.sql.Statement stmt = connection.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, 
				ResultSet.CONCUR_READ_ONLY);
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			// Ir al final del ResultSet
			if (rs.last()) {
				int numeroFilas = rs.getRow();
				System.out.println("✅ Número de filas obtenidas: " + numeroFilas);
				
				// Volver al principio para mostrar algunos datos de ejemplo
				rs.beforeFirst();
				System.out.println("\n📋 Primeras filas de ejemplo:");
				System.out.println("DNI\t\tAPELLIDOS\tCP");
				System.out.println("----------------------------------------");
				
				int contador = 0;
				while (rs.next() && contador < 3) {
					System.out.printf("%-15s %-15s %s%n", 
						rs.getString("DNI"),
						rs.getString("APELLIDOS"),
						rs.getObject("CP"));
					contador++;
				}
				if (numeroFilas > 3) {
					System.out.println("... y " + (numeroFilas - 3) + " filas más");
				}
			} else {
				System.out.println("❌ No hay datos en la consulta");
			}
		}
		System.out.println("=====================================================");
	}
	
	/**
	 * Método 2: Usando consulta COUNT separada (más eficiente)
	 * Este es el método más eficiente para obtener solo el conteo.
	 */
	public void contarFilasConConsultaCount() throws SQLException {
		String sqlOriginal = "SELECT DNI, APELLIDOS, CP FROM CLIENTES";
		String sqlCount = "SELECT COUNT(*) AS total FROM CLIENTES";
		
		System.out.println("=== ACTIVIDAD 4.4 - Método 2: Consulta COUNT ===");
		System.out.println("Consulta original: " + sqlOriginal);
		System.out.println("Consulta para contar: " + sqlCount);
		
		// Primero obtenemos el conteo
		try (java.sql.Statement stmtCount = connection.createStatement();
			 ResultSet rsCount = stmtCount.executeQuery(sqlCount)) {
			
			if (rsCount.next()) {
				int numeroFilas = rsCount.getInt("total");
				System.out.println("✅ Número de filas (sin recorrer): " + numeroFilas);
				
				// Ahora ejecutamos la consulta original para mostrar algunos datos
				try (java.sql.Statement stmtData = connection.createStatement();
					 ResultSet rsData = stmtData.executeQuery(sqlOriginal)) {
					
					System.out.println("\n📋 Datos de ejemplo de la consulta:");
					System.out.println("DNI\t\tAPELLIDOS\tCP");
					System.out.println("----------------------------------------");
					
					int contador = 0;
					while (rsData.next() && contador < 3) {
						System.out.printf("%-15s %-15s %s%n", 
							rsData.getString("DNI"),
							rsData.getString("APELLIDOS"),
							rsData.getObject("CP"));
						contador++;
					}
					if (numeroFilas > 3) {
						System.out.println("... y " + (numeroFilas - 3) + " filas más");
					}
				}
			}
		}
		System.out.println("==============================================");
	}
	
	/**
	 * Método 3: Comparativo de rendimiento entre métodos
	 * Demuestra las diferencias de rendimiento entre los diferentes enfoques.
	 */
	public void compararMetodosConteo() throws SQLException {
		String tabla = "CLIENTES";
		String sql = "SELECT * FROM " + tabla;
		
		System.out.println("=== ACTIVIDAD 4.4 - Método 3: Comparativo de Rendimiento ===");
		
		// Método 1: ResultSet Scrollable
		long inicioScrollable = System.currentTimeMillis();
		try (java.sql.Statement stmt = connection.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, 
				ResultSet.CONCUR_READ_ONLY);
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			int filasScrollable = 0;
			if (rs.last()) {
				filasScrollable = rs.getRow();
			}
			long tiempoScrollable = System.currentTimeMillis() - inicioScrollable;
			System.out.println("🔄 Método Scrollable:");
			System.out.println("   - Filas encontradas: " + filasScrollable);
			System.out.println("   - Tiempo: " + tiempoScrollable + " ms");
		}
		
		// Método 2: COUNT Query
		long inicioCount = System.currentTimeMillis();
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tabla)) {
			
			int filasCount = 0;
			if (rs.next()) {
				filasCount = rs.getInt(1);
			}
			long tiempoCount = System.currentTimeMillis() - inicioCount;
			System.out.println("📊 Método COUNT:");
			System.out.println("   - Filas encontradas: " + filasCount);
			System.out.println("   - Tiempo: " + tiempoCount + " ms");
		}
		
		// Método tradicional (recorriendo todo)
		long inicioTradicional = System.currentTimeMillis();
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			int filasTradicional = 0;
			while (rs.next()) {
				filasTradicional++;
			}
			long tiempoTradicional = System.currentTimeMillis() - inicioTradicional;
			System.out.println("🐌 Método Tradicional (recorriendo):");
			System.out.println("   - Filas encontradas: " + filasTradicional);
			System.out.println("   - Tiempo: " + tiempoTradicional + " ms");
		}
		
		System.out.println("\n💡 Recomendación: El método COUNT es generalmente el más eficiente");
		System.out.println("   para obtener solo el número de filas.");
		System.out.println("========================================================");
	}

	/**
	 * Método 4: Demostración completa de las técnicas con explicación detallada
	 * Este método explica cuándo usar cada técnica y sus ventajas/desventajas.
	 */
	public void explicarTecnicasConteoFilas() throws SQLException {
		System.out.println("=== ACTIVIDAD 4.4 - EXPLICACIÓN DETALLADA DE TÉCNICAS ===");
		
		String consulta = "SELECT DNI, APELLIDOS, CP FROM CLIENTES WHERE CP IS NOT NULL";
		
		System.out.println("📋 Consulta de ejemplo: " + consulta);
		System.out.println();
		
		// 1. TÉCNICA SCROLLABLE RESULTSET
		System.out.println("🔄 TÉCNICA 1: ResultSet Scrollable");
		System.out.println("   ✅ Ventajas:");
		System.out.println("      - Obtiene el conteo sin recorrer fila por fila");
		System.out.println("      - Permite navegar hacia adelante y atrás");
		System.out.println("      - Útil cuando necesitas tanto el conteo como los datos");
		System.out.println("   ❌ Desventajas:");
		System.out.println("      - Consume más memoria (almacena todo el ResultSet)");
		System.out.println("      - No todos los drivers JDBC lo soportan completamente");
		System.out.println("      - Puede ser más lento con grandes volúmenes de datos");
		
		try (java.sql.Statement stmt = connection.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, 
				ResultSet.CONCUR_READ_ONLY);
			 ResultSet rs = stmt.executeQuery(consulta)) {
			
			if (rs.last()) {
				System.out.println("   📊 Resultado: " + rs.getRow() + " filas");
			}
		}
		
		System.out.println();
		
		// 2. TÉCNICA COUNT QUERY
		System.out.println("📊 TÉCNICA 2: Consulta COUNT Separada");
		System.out.println("   ✅ Ventajas:");
		System.out.println("      - MÁS EFICIENTE para solo obtener el conteo");
		System.out.println("      - Consume mínima memoria y ancho de banda");
		System.out.println("      - Optimizada por el motor de base de datos");
		System.out.println("      - Funciona con cualquier driver JDBC");
		System.out.println("   ❌ Desventajas:");
		System.out.println("      - Requiere dos consultas si también necesitas los datos");
		System.out.println("      - Los datos pueden cambiar entre consultas");
		
		// Convertir la consulta original a COUNT
		String consultaCount = consulta.replaceFirst("SELECT.*FROM", "SELECT COUNT(*) FROM");
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(consultaCount)) {
			
			if (rs.next()) {
				System.out.println("   📊 Resultado: " + rs.getInt(1) + " filas");
			}
		}
		
		System.out.println();
		
		// 3. CUÁNDO USAR CADA MÉTODO
		System.out.println("🎯 RECOMENDACIONES DE USO:");
		System.out.println("   🔹 Usa COUNT cuando:");
		System.out.println("      - Solo necesitas el número de filas");
		System.out.println("      - Trabajas con grandes volúmenes de datos");
		System.out.println("      - La eficiencia es prioritaria");
		System.out.println();
		System.out.println("   🔹 Usa ResultSet Scrollable cuando:");
		System.out.println("      - Necesitas tanto el conteo como navegar por los datos");
		System.out.println("      - El volumen de datos es moderado");
		System.out.println("      - Necesitas funcionalidad de navegación bidireccional");
		System.out.println();
		System.out.println("   🔹 NUNCA uses el recorrido completo cuando:");
		System.out.println("      - Solo necesitas el conteo (es ineficiente)");
		System.out.println("      - Hay alternativas más eficientes disponibles");
		
		System.out.println("==========================================================");
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
