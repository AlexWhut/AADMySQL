package id.morantesbryan.dao;

import id.morantesbryan.pojos.Cliente;
import id.morantesbryan.pojos.ClienteNuevo;
import id.morantesbryan.pojos.Company;
import id.morantesbryan.pojos.LineaFactura;
import id.morantesbryan.pojos.ResultadoListado;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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

	/**
	 * ACTIVIDAD 4.5: Método que consulta datos de varios clientes usando sentencias preparadas.
	 * Realiza una consulta individual por cada DNI especificado usando PreparedStatement.
	 * 
	 * @param dnis Array de DNIs de los clientes a consultar
	 * @throws SQLException Si ocurre un error en la base de datos
	 */
	public void consultarClientesPorDNI(String[] dnis) throws SQLException {
		// Validación de entrada
		if (dnis == null || dnis.length == 0) {
			System.out.println("❌ No se proporcionaron DNIs para consultar.");
			return;
		}

		System.out.println("=== ACTIVIDAD 4.5 - CONSULTA DE CLIENTES CON PREPARED STATEMENT ===");
		System.out.println("📋 Consultando " + dnis.length + " cliente(s) individualmente...");
		System.out.println();

		// Sentencia SQL preparada - La consulta SELECT con parámetro ?
		// El ? será reemplazado por cada DNI específico en cada iteración
		String sqlConsulta = "SELECT * FROM CLIENTES WHERE DNI = ?";
		System.out.println("🔍 Consulta SQL: " + sqlConsulta);
		System.out.println();

		// Crear la sentencia preparada UNA SOLA VEZ fuera del bucle
		// Esto es más eficiente que crear una nueva PreparedStatement en cada iteración
		try (PreparedStatement pstmt = connection.prepareStatement(sqlConsulta)) {
			
			// Contadores para estadísticas
			int clientesEncontrados = 0;
			int clientesNoEncontrados = 0;

			// Iterar sobre cada DNI proporcionado
			for (int i = 0; i < dnis.length; i++) {
				String dniActual = dnis[i];
				
				System.out.println("🔎 Consulta " + (i + 1) + "/" + dnis.length + " - DNI: " + dniActual);
				
				// Validar que el DNI no sea nulo o vacío
				if (dniActual == null || dniActual.trim().isEmpty()) {
					System.out.println("   ⚠️  DNI inválido (nulo o vacío) - Saltando...");
					clientesNoEncontrados++;
					System.out.println();
					continue;
				}

				// Establecer el parámetro ? con el DNI actual
				// setString(1, valor) significa: reemplazar el primer ? con 'valor'
				pstmt.setString(1, dniActual.trim());

				// Ejecutar la consulta preparada
				// Como consultamos por clave primaria, esperamos 0 o 1 fila máximo
				try (ResultSet rs = pstmt.executeQuery()) {
					
					// Verificar si se encontró el cliente
					if (rs.next()) {
						// Cliente encontrado - extraer datos del ResultSet
						String dni = rs.getString("DNI");
						String apellidos = rs.getString("APELLIDOS");
						
						// Manejar valores NULL en la columna CP
						int cp = rs.getInt("CP");
						String cpTexto = rs.wasNull() ? "null" : String.valueOf(cp);
						
						// Mostrar los datos del cliente encontrado
						System.out.println("   ✅ Cliente encontrado:");
						System.out.println("      - DNI: " + dni);
						System.out.println("      - Apellidos: " + apellidos);
						System.out.println("      - CP: " + cpTexto);
						
						clientesEncontrados++;
					} else {
						// No se encontró ningún cliente con ese DNI
						System.out.println("   ❌ Cliente no encontrado en la base de datos");
						clientesNoEncontrados++;
					}
				}
				
				System.out.println(); // Línea en blanco para separar resultados
			}

			// Mostrar resumen estadístico final
			mostrarResumenConsulta(clientesEncontrados, clientesNoEncontrados, dnis.length);
		}
	}

	/**
	 * Método sobrecargado que consulta TODOS los clientes de la tabla.
	 * Primero obtiene todos los DNIs y luego los consulta individualmente.
	 */
	public void consultarTodosLosClientesIndividualmente() throws SQLException {
		System.out.println("=== ACTIVIDAD 4.5 - CONSULTA DE TODOS LOS CLIENTES ===");
		System.out.println("🔍 Obteniendo lista de todos los DNIs...");

		// Primero obtener todos los DNIs existentes
		List<String> listaDnis = new ArrayList<>();
		String sqlObtenerDnis = "SELECT DNI FROM CLIENTES ORDER BY DNI";

		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sqlObtenerDnis)) {
			
			while (rs.next()) {
				listaDnis.add(rs.getString("DNI"));
			}
		}

		if (listaDnis.isEmpty()) {
			System.out.println("❌ No hay clientes en la tabla CLIENTES.");
			return;
		}

		System.out.println("📊 Se encontraron " + listaDnis.size() + " cliente(s) en total.");
		System.out.println();

		// Convertir List a Array y llamar al método principal
		String[] arrayDnis = listaDnis.toArray(new String[0]);
		consultarClientesPorDNI(arrayDnis);
	}

	/**
	 * Método auxiliar para mostrar el resumen estadístico de la consulta.
	 * 
	 * @param encontrados Número de clientes encontrados
	 * @param noEncontrados Número de clientes no encontrados
	 * @param total Total de consultas realizadas
	 */
	private void mostrarResumenConsulta(int encontrados, int noEncontrados, int total) {
		System.out.println("📊 RESUMEN DE CONSULTAS:");
		System.out.println("   🎯 Total de consultas realizadas: " + total);
		System.out.println("   ✅ Clientes encontrados: " + encontrados);
		System.out.println("   ❌ Clientes no encontrados: " + noEncontrados);
		
		if (total > 0) {
			double porcentajeExito = (encontrados * 100.0) / total;
			System.out.println("   📈 Porcentaje de éxito: " + String.format("%.1f%%", porcentajeExito));
		}
		
		System.out.println();
		System.out.println("💡 VENTAJAS DE LAS SENTENCIAS PREPARADAS:");
		System.out.println("   🔒 Protección contra inyección SQL");
		System.out.println("   🚀 Mejor rendimiento al reutilizar la consulta compilada");
		System.out.println("   🎯 Código más limpio y mantenible");
		System.out.println("   ✅ Manejo automático de tipos de datos y caracteres especiales");
		System.out.println("=================================================================");
	}

	/**
	 * Método de demostración que compara diferentes enfoques de consulta.
	 * Muestra las diferencias entre Statement normal vs PreparedStatement.
	 */
	public void demostrarVentajasPreparedStatement() throws SQLException {
		System.out.println("=== DEMOSTRACIÓN: PREPARED STATEMENT VS STATEMENT NORMAL ===");
		
		String[] dnisPrueba = {"78901234X", "89012345E"};
		
		System.out.println("⚠️  ENFOQUE INSEGURO - Statement Normal (NO recomendado):");
		System.out.println("   String sql = \"SELECT * FROM CLIENTES WHERE DNI = '\" + dni + \"'\";");
		System.out.println("   ❌ Vulnerable a inyección SQL");
		System.out.println("   ❌ Menos eficiente (recompila cada vez)");
		System.out.println();
		
		System.out.println("✅ ENFOQUE SEGURO - PreparedStatement (RECOMENDADO):");
		System.out.println("   String sql = \"SELECT * FROM CLIENTES WHERE DNI = ?\";");
		System.out.println("   ✅ Protegido contra inyección SQL");
		System.out.println("   ✅ Más eficiente (precompilado)");
		System.out.println();
		
		// Demostrar el uso correcto con PreparedStatement
		String sql = "SELECT * FROM CLIENTES WHERE DNI = ?";
		try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
			
			for (String dni : dnisPrueba) {
				System.out.println("🔍 Consultando DNI: " + dni);
				
				// Forma segura: usar setString para evitar inyección SQL
				pstmt.setString(1, dni);
				
				try (ResultSet rs = pstmt.executeQuery()) {
					if (rs.next()) {
						System.out.println("   ✅ Cliente: " + rs.getString("APELLIDOS"));
					} else {
						System.out.println("   ❌ No encontrado");
					}
				}
			}
		}
		
		System.out.println("\n💡 BUENAS PRÁCTICAS IMPLEMENTADAS:");
		System.out.println("   🔹 Usar try-with-resources para gestión automática de recursos");
		System.out.println("   🔹 Una sola PreparedStatement reutilizada múltiples veces");
		System.out.println("   🔹 Validación de datos de entrada");
		System.out.println("   🔹 Manejo adecuado de valores NULL");
		System.out.println("   🔹 Comentarios claros explicando cada paso");
		System.out.println("===================================================");
	}

	/**
	 * =================== ACTIVIDAD 4.6 ===================
	 * Métodos para gestión de la tabla COMPANIES y operaciones batch.
	 */

	/**
	 * ACTIVIDAD 4.6 - Método 1: Crear tabla COMPANIES.
	 * Crea la tabla COMPANIES con los campos especificados si no existe.
	 * 
	 * Estructura de la tabla:
	 * - CIF: VARCHAR(9) PRIMARY KEY - Código de identificación fiscal (8 números + 1 letra)
	 * - NOMBRE: VARCHAR(255) NOT NULL - Nombre de la compañía
	 * - SECTOR: VARCHAR(100) NOT NULL - Sector al que se dedica la compañía
	 * 
	 * @throws SQLException Si ocurre un error al crear la tabla
	 */
	public void crearTablaCompanies() throws SQLException {
		System.out.println("=== ACTIVIDAD 4.6 - Creación de Tabla COMPANIES ===");
		
		// SQL para crear la tabla COMPANIES
		String sqlCrearTabla = """
			CREATE TABLE IF NOT EXISTS COMPANIES (
				CIF VARCHAR(9) PRIMARY KEY COMMENT 'Código de Identificación Fiscal (8 números + 1 letra)',
				NOMBRE VARCHAR(255) NOT NULL COMMENT 'Nombre de la compañía',
				SECTOR VARCHAR(100) NOT NULL COMMENT 'Sector al que se dedica la compañía',
				CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Fecha de creación del registro'
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
			  COMMENT='Tabla que almacena información de compañías'
		""";
		
		try (java.sql.Statement stmt = connection.createStatement()) {
			System.out.println("🔧 Creando tabla COMPANIES...");
			stmt.execute(sqlCrearTabla);
			System.out.println("✅ Tabla COMPANIES creada exitosamente (o ya existía)");
			
			// Mostrar estructura de la tabla
			mostrarEstructuraTabla("COMPANIES");
			
		} catch (SQLException e) {
			System.err.println("❌ Error al crear la tabla COMPANIES: " + e.getMessage());
			throw e;
		}
		
		System.out.println("=============================================");
	}

	/**
	 * ACTIVIDAD 4.6 - Método 2: Insertar compañías en modo batch con transacción controlada.
	 * 
	 * Este método implementa las mejores prácticas para inserciones masivas:
	 * - Uso de PreparedStatement para seguridad contra inyección SQL
	 * - Operaciones batch para mejor rendimiento
	 * - Transacciones controladas con commit/rollback automático
	 * - Validación de datos antes de la inserción
	 * - Manejo robusto de errores
	 * 
	 * @param companies Lista de compañías a insertar
	 * @return Número de compañías insertadas exitosamente
	 * @throws SQLException Si ocurre un error irrecuperable
	 */
	public int insertarCompaniesEnBatch(List<Company> companies) throws SQLException {
		// Validación inicial
		if (companies == null || companies.isEmpty()) {
			System.out.println("⚠️ No se proporcionaron compañías para insertar.");
			return 0;
		}

		System.out.println("=== ACTIVIDAD 4.6 - Inserción Batch de Compañías ===");
		System.out.println("📊 Preparando inserción de " + companies.size() + " compañía(s)...");

		// SQL de inserción con parámetros
		String sqlInsert = "INSERT INTO COMPANIES (CIF, NOMBRE, SECTOR) VALUES (?, ?, ?)";
		
		// Variables para estadísticas
		int companiesValidadas = 0;
		int companiesInsertadas = 0;
		List<String> erroresValidacion = new ArrayList<>();

		// Guardar el estado original del autoCommit
		boolean autoCommitOriginal = connection.getAutoCommit();
		
		try {
			// Desactivar autoCommit para control manual de transacciones
			connection.setAutoCommit(false);
			System.out.println("🔄 Transacción iniciada (autoCommit = false)");

			try (PreparedStatement pstmt = connection.prepareStatement(sqlInsert)) {
				
				// Validar y preparar cada compañía para el batch
				for (int i = 0; i < companies.size(); i++) {
					Company company = companies.get(i);
					
					System.out.println("🔍 Validando compañía " + (i + 1) + "/" + companies.size() + ": " + 
									 (company != null ? company.getCif() : "null"));
					
					// Validación completa de la compañía
					String errorValidacion = validarCompany(company);
					if (errorValidacion != null) {
						erroresValidacion.add("Compañía " + (i + 1) + ": " + errorValidacion);
						System.out.println("   ❌ " + errorValidacion);
						continue;
					}
					
					// Agregar al batch si la validación fue exitosa
					pstmt.setString(1, company.getCif().toUpperCase().trim());
					pstmt.setString(2, company.getNombre().trim());
					pstmt.setString(3, company.getSector().trim());
					pstmt.addBatch();
					
					companiesValidadas++;
					System.out.println("   ✅ Compañía válida agregada al batch");
				}

				// Verificar si hay compañías válidas para insertar
				if (companiesValidadas == 0) {
					System.out.println("❌ No hay compañías válidas para insertar");
					connection.rollback();
					return 0;
				}

				System.out.println("\n🚀 Ejecutando batch de " + companiesValidadas + " compañía(s)...");
				
				// Ejecutar el batch
				int[] resultados = pstmt.executeBatch();
				
				// Analizar resultados del batch
				for (int i = 0; i < resultados.length; i++) {
					if (resultados[i] > 0 || resultados[i] == java.sql.Statement.SUCCESS_NO_INFO) {
						companiesInsertadas++;
					}
				}

				// Si todo salió bien, confirmar la transacción
				connection.commit();
				System.out.println("✅ TRANSACCIÓN CONFIRMADA (COMMIT)");
				
				// Mostrar estadísticas finales
				mostrarEstadisticasInsercion(companies.size(), companiesValidadas, 
										   companiesInsertadas, erroresValidacion);

			} catch (SQLException e) {
				// Error durante la ejecución del batch - hacer rollback
				System.err.println("❌ Error durante la inserción batch: " + e.getMessage());
				connection.rollback();
				System.err.println("🔄 TRANSACCIÓN REVERTIDA (ROLLBACK)");
				throw e;
			}

		} finally {
			// Restaurar el estado original del autoCommit
			connection.setAutoCommit(autoCommitOriginal);
			System.out.println("🔄 AutoCommit restaurado a: " + autoCommitOriginal);
		}

		return companiesInsertadas;
	}

	/**
	 * Método auxiliar para validar una compañía antes de la inserción.
	 * 
	 * @param company Compañía a validar
	 * @return null si es válida, mensaje de error si no es válida
	 */
	private String validarCompany(Company company) {
		if (company == null) {
			return "Compañía es null";
		}
		
		if (!company.isComplete()) {
			return "Compañía incompleta (faltan campos obligatorios)";
		}
		
		if (!company.isValidCif()) {
			return "CIF inválido (debe tener 8 números + 1 letra): " + company.getCif();
		}
		
		if (company.getNombre().length() > 255) {
			return "Nombre demasiado largo (máximo 255 caracteres)";
		}
		
		if (company.getSector().length() > 100) {
			return "Sector demasiado largo (máximo 100 caracteres)";
		}
		
		return null; // Validación exitosa
	}

	/**
	 * Método auxiliar para mostrar la estructura de una tabla.
	 * 
	 * @param nombreTabla Nombre de la tabla a describir
	 */
	private void mostrarEstructuraTabla(String nombreTabla) throws SQLException {
		String sqlDescribe = "DESCRIBE " + nombreTabla;
		
		System.out.println("\n📋 Estructura de la tabla " + nombreTabla + ":");
		System.out.println("Campo\t\tTipo\t\tNull\tKey\tDefault");
		System.out.println("------------------------------------------------------------");
		
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sqlDescribe)) {
			
			while (rs.next()) {
				System.out.printf("%-15s %-15s %-8s %-8s %s%n",
					rs.getString("Field"),
					rs.getString("Type"),
					rs.getString("Null"),
					rs.getString("Key"),
					rs.getString("Default"));
			}
		}
		System.out.println();
	}

	/**
	 * Método auxiliar para mostrar estadísticas de la inserción.
	 */
	private void mostrarEstadisticasInsercion(int total, int validadas, int insertadas, 
											List<String> errores) {
		System.out.println("\n📊 ESTADÍSTICAS DE INSERCIÓN:");
		System.out.println("   📝 Total de compañías procesadas: " + total);
		System.out.println("   ✅ Compañías validadas: " + validadas);
		System.out.println("   💾 Compañías insertadas: " + insertadas);
		System.out.println("   ❌ Compañías rechazadas: " + (total - validadas));
		
		if (total > 0) {
			double porcentajeExito = (insertadas * 100.0) / total;
			System.out.println("   📈 Porcentaje de éxito: " + String.format("%.1f%%", porcentajeExito));
		}
		
		if (!errores.isEmpty()) {
			System.out.println("\n⚠️ ERRORES DE VALIDACIÓN:");
			for (String error : errores) {
				System.out.println("   • " + error);
			}
		}
		
		System.out.println("\n💡 VENTAJAS DEL MÉTODO IMPLEMENTADO:");
		System.out.println("   🔒 Seguridad: PreparedStatement previene inyección SQL");
		System.out.println("   🚀 Rendimiento: Operaciones batch para inserciones masivas");
		System.out.println("   🛡️ Robustez: Transacciones controladas con rollback automático");
		System.out.println("   ✅ Validación: Verificación completa de datos antes de insertar");
		System.out.println("   📊 Información: Estadísticas detalladas del proceso");
		System.out.println("=======================================================");
	}

	/**
	 * Método auxiliar para consultar y mostrar las compañías insertadas.
	 * 
	 * @throws SQLException Si ocurre un error en la consulta
	 */
	public void mostrarCompaniesInsertadas() throws SQLException {
		System.out.println("=== COMPAÑÍAS EN LA BASE DE DATOS ===");
		
		String sql = "SELECT CIF, NOMBRE, SECTOR, CREATED_AT FROM COMPANIES ORDER BY CREATED_AT DESC";
		
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			System.out.println("CIF\t\tNOMBRE\t\t\tSECTOR\t\tFECHA CREACIÓN");
			System.out.println("-------------------------------------------------------------------------");
			
			int contador = 0;
			while (rs.next()) {
				contador++;
				System.out.printf("%-12s %-25s %-20s %s%n",
					rs.getString("CIF"),
					rs.getString("NOMBRE"),
					rs.getString("SECTOR"),
					rs.getTimestamp("CREATED_AT"));
			}
			
			if (contador == 0) {
				System.out.println("   No hay compañías registradas.");
			} else {
				System.out.println("\nTotal de compañías: " + contador);
			}
		}
		
		System.out.println("====================================");
	}

	/**
	 * Método de demostración que muestra diferentes escenarios de batch insert.
	 * Incluye casos de éxito, error y recuperación.
	 */
	public void demostrarEscenariosCompanies() throws SQLException {
		System.out.println("=== DEMOSTRACIÓN: ESCENARIOS DE BATCH INSERT ===");
		
		// Escenario 1: Batch completamente válido
		System.out.println("🎯 Escenario 1: Todas las compañías son válidas");
		List<Company> companiesValidas = Arrays.asList(
			new Company("10101010A", "Empresa Ejemplo 1", "Retail"),
			new Company("20202020B", "Empresa Ejemplo 2", "Servicios"),
			new Company("30303030C", "Empresa Ejemplo 3", "Industrial")
		);
		insertarCompaniesEnBatch(companiesValidas);
		
		// Escenario 2: Batch con errores mezclados
		System.out.println("\n🎯 Escenario 2: Mezcla de válidas e inválidas");
		List<Company> companiesMixtas = Arrays.asList(
			new Company("40404040D", "Empresa Válida", "Tecnología"),
			new Company("INVALID02", "CIF Malo", "Tech"), // CIF inválido
			new Company("50505050E", "Otra Válida", "Consultoría")
		);
		insertarCompaniesEnBatch(companiesMixtas);
		
		// Escenario 3: Intento de duplicados (debería fallar por clave primaria)
		System.out.println("\n🎯 Escenario 3: Intento de insertar duplicados");
		List<Company> companiesDuplicadas = Arrays.asList(
			new Company("10101010A", "Empresa Duplicada", "Retail") // CIF ya existe
		);
		
		try {
			insertarCompaniesEnBatch(companiesDuplicadas);
		} catch (SQLException e) {
			System.out.println("❌ Error esperado por duplicado: " + e.getMessage());
			System.out.println("✅ El sistema maneja correctamente los duplicados");
		}
		
		System.out.println("\n📊 Estado final de la tabla:");
		mostrarCompaniesInsertadas();
		
		System.out.println("==============================================");
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
