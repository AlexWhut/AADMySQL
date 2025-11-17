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
				// Duplicate key or other integrity constraint violation â€” warn and continue
				System.err.println("Advertencia: restricciÃ³n de integridad al ejecutar la sentencia: " + ex.getMessage());
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
	 * se insertarÃ¡ como fallback.
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
	 * ACTIVIDAD 4.3: MÃ©todo que ejecuta la consulta SQL para obtener nombres completos 
	 * de empleados y los muestra en orden inverso (del Ãºltimo al primero).
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
		
		// Invertimos la lista para mostrar del Ãºltimo al primero
		Collections.reverse(nombres);
		
		// Mostramos los resultados en orden inverso
		System.out.println("=== EMPLEADOS EN ORDEN INVERSO ===");
		System.out.println("Total de empleados: " + nombres.size());
		System.out.println("Mostrando del Ãºltimo al primero:");
		System.out.println("================================");
		
		for (int i = 0; i < nombres.size(); i++) {
			System.out.println((i + 1) + ". " + nombres.get(i));
		}
		
		System.out.println("================================");
	}

	/**
	 * MÃ©todo alternativo mÃ¡s eficiente que obtiene los nombres ya en orden inverso
	 * desde la base de datos usando ORDER BY DESC, pero manteniendo la consulta base.
	 */
	public void mostrarEmpleadosOrdenInversoAlternativo() throws SQLException {
		// Consulta original con ORDER BY para obtener resultados ya ordenados inversamente
		String sql = "SELECT CONCAT(first_name, ' ', last_name) AS name FROM employees ORDER BY name DESC";
		
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			System.out.println("=== EMPLEADOS EN ORDEN INVERSO (MÃ©todo alternativo) ===");
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
	 * ACTIVIDAD 4.4: MÃ©todos para averiguar el nÃºmero de filas de una consulta
	 * sin recorrer todo el ResultSet para contarlas.
	 */
	
	/**
	 * MÃ©todo 1: Usando ResultSet scrollable con last() y getRow()
	 * Este mÃ©todo permite obtener el nÃºmero de filas sin recorrer todo el ResultSet.
	 */
	public void contarFilasConScrollableResultSet() throws SQLException {
		String sql = "SELECT DNI, APELLIDOS, CP FROM CLIENTES";
		
		System.out.println("=== ACTIVIDAD 4.4 - MÃ©todo 1: ResultSet Scrollable ===");
		System.out.println("Consulta: " + sql);
		
		// Crear Statement con ResultSet scrollable
		try (java.sql.Statement stmt = connection.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, 
				ResultSet.CONCUR_READ_ONLY);
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			// Ir al final del ResultSet
			if (rs.last()) {
				int numeroFilas = rs.getRow();
				System.out.println("NÃºmero de filas obtenidas: " + numeroFilas);
				
				// Volver al principio para mostrar algunos datos de ejemplo
				rs.beforeFirst();
				System.out.println("\nPrimeras filas de ejemplo:");
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
					System.out.println("... y " + (numeroFilas - 3) + " filas mÃ¡s");
				}
			} else {
				System.out.println("No hay datos en la consulta");
			}
		}
		System.out.println("=====================================================");
	}
	
	/**
	 * MÃ©todo 2: Usando consulta COUNT separada (mÃ¡s eficiente)
	 * Este es el mÃ©todo mÃ¡s eficiente para obtener solo el conteo.
	 */
	public void contarFilasConConsultaCount() throws SQLException {
		String sqlOriginal = "SELECT DNI, APELLIDOS, CP FROM CLIENTES";
		String sqlCount = "SELECT COUNT(*) AS total FROM CLIENTES";
		
		System.out.println("=== ACTIVIDAD 4.4 - MÃ©todo 2: Consulta COUNT ===");
		System.out.println("Consulta original: " + sqlOriginal);
		System.out.println("Consulta para contar: " + sqlCount);
		
		// Primero obtenemos el conteo
		try (java.sql.Statement stmtCount = connection.createStatement();
			 ResultSet rsCount = stmtCount.executeQuery(sqlCount)) {
			
			if (rsCount.next()) {
				int numeroFilas = rsCount.getInt("total");
				System.out.println("NÃºmero de filas (sin recorrer): " + numeroFilas);
				
				// Ahora ejecutamos la consulta original para mostrar algunos datos
				try (java.sql.Statement stmtData = connection.createStatement();
					 ResultSet rsData = stmtData.executeQuery(sqlOriginal)) {
					
					System.out.println("\nDatos de ejemplo de la consulta:");
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
						System.out.println("... y " + (numeroFilas - 3) + " filas mÃ¡s");
					}
				}
			}
		}
		System.out.println("==============================================");
	}
	
	/**
	 * MÃ©todo 3: Comparativo de rendimiento entre mÃ©todos
	 * Demuestra las diferencias de rendimiento entre los diferentes enfoques.
	 */
	public void compararMetodosConteo() throws SQLException {
		String tabla = "CLIENTES";
		String sql = "SELECT * FROM " + tabla;
		
		System.out.println("=== ACTIVIDAD 4.4 - MÃ©todo 3: Comparativo de Rendimiento ===");
		
		// MÃ©todo 1: ResultSet Scrollable
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
			System.out.println("MÃ©todo Scrollable:");
			System.out.println("   - Filas encontradas: " + filasScrollable);
			System.out.println("   - Tiempo: " + tiempoScrollable + " ms");
		}
		
		// MÃ©todo 2: COUNT Query
		long inicioCount = System.currentTimeMillis();
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tabla)) {
			
			int filasCount = 0;
			if (rs.next()) {
				filasCount = rs.getInt(1);
			}
			long tiempoCount = System.currentTimeMillis() - inicioCount;
			System.out.println("MÃ©todo COUNT:");
			System.out.println("   - Filas encontradas: " + filasCount);
			System.out.println("   - Tiempo: " + tiempoCount + " ms");
		}
		
		// MÃ©todo tradicional (recorriendo todo)
		long inicioTradicional = System.currentTimeMillis();
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			int filasTradicional = 0;
			while (rs.next()) {
				filasTradicional++;
			}
			long tiempoTradicional = System.currentTimeMillis() - inicioTradicional;
			System.out.println(" MÃ©todo Tradicional (recorriendo):");
			System.out.println("   - Filas encontradas: " + filasTradicional);
			System.out.println("   - Tiempo: " + tiempoTradicional + " ms");
		}
		
		System.out.println("\n RecomendaciÃ³n: El mÃ©todo COUNT es generalmente el mÃ¡s eficiente");
		System.out.println("   para obtener solo el nÃºmero de filas.");
		System.out.println("========================================================");
	}

	/**
	 * MÃ©todo 4: DemostraciÃ³n completa de las tÃ©cnicas con explicaciÃ³n detallada
	 * Este mÃ©todo explica cuÃ¡ndo usar cada tÃ©cnica y sus ventajas/desventajas.
	 */
	public void explicarTecnicasConteoFilas() throws SQLException {
		System.out.println("=== ACTIVIDAD 4.4 - EXPLICACIÃ“N DETALLADA DE TÃ‰CNICAS ===");
		
		String consulta = "SELECT DNI, APELLIDOS, CP FROM CLIENTES WHERE CP IS NOT NULL";
		
		System.out.println(" Consulta de ejemplo: " + consulta);
		System.out.println();
		
		// 1. TÃ‰CNICA SCROLLABLE RESULTSET
		System.out.println(" TÃ‰CNICA 1: ResultSet Scrollable");
		System.out.println("    Ventajas:");
		System.out.println("      - Obtiene el conteo sin recorrer fila por fila");
		System.out.println("      - Permite navegar hacia adelante y atrÃ¡s");
		System.out.println("      - Ãštil cuando necesitas tanto el conteo como los datos");
		System.out.println("    Desventajas:");
		System.out.println("      - Consume mÃ¡s memoria (almacena todo el ResultSet)");
		System.out.println("      - No todos los drivers JDBC lo soportan completamente");
		System.out.println("      - Puede ser mÃ¡s lento con grandes volÃºmenes de datos");
		
		try (java.sql.Statement stmt = connection.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, 
				ResultSet.CONCUR_READ_ONLY);
			 ResultSet rs = stmt.executeQuery(consulta)) {
			
			if (rs.last()) {
				System.out.println("    Resultado: " + rs.getRow() + " filas");
			}
		}
		
		System.out.println();
		
		// 2. TÃ‰CNICA COUNT QUERY
		System.out.println(" TÃ‰CNICA 2: Consulta COUNT Separada");
		System.out.println("    Ventajas:");
		System.out.println("      - MÃS EFICIENTE para solo obtener el conteo");
		System.out.println("      - Consume mÃ­nima memoria y ancho de banda");
		System.out.println("      - Optimizada por el motor de base de datos");
		System.out.println("      - Funciona con cualquier driver JDBC");
		System.out.println("    Desventajas:");
		System.out.println("      - Requiere dos consultas si tambiÃ©n necesitas los datos");
		System.out.println("      - Los datos pueden cambiar entre consultas");
		
		// Convertir la consulta original a COUNT
		String consultaCount = consulta.replaceFirst("SELECT.*FROM", "SELECT COUNT(*) FROM");
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(consultaCount)) {
			
			if (rs.next()) {
				System.out.println("    Resultado: " + rs.getInt(1) + " filas");
			}
		}
		
		System.out.println();
		
		// 3. CUÃNDO USAR CADA MÃ‰TODO
		System.out.println("RECOMENDACIONES DE USO:");
		System.out.println("   Usa COUNT cuando:");
		System.out.println("      - Solo necesitas el nÃºmero de filas");
		System.out.println("      - Trabajas con grandes volÃºmenes de datos");
		System.out.println("      - La eficiencia es prioritaria");
		System.out.println();
		System.out.println("   Usa ResultSet Scrollable cuando:");
		System.out.println("      - Necesitas tanto el conteo como navegar por los datos");
		System.out.println("      - El volumen de datos es moderado");
		System.out.println("      - Necesitas funcionalidad de navegaciÃ³n bidireccional");
		System.out.println();
		System.out.println("   NUNCA uses el recorrido completo cuando:");
		System.out.println("      - Solo necesitas el conteo (es ineficiente)");
		System.out.println("      - Hay alternativas mÃ¡s eficientes disponibles");
		
		System.out.println("==========================================================");
	}

	/**
	 * ACTIVIDAD 4.5: MÃ©todo que consulta datos de varios clientes usando sentencias preparadas.
	 * Realiza una consulta individual por cada DNI especificado usando PreparedStatement.
	 * 
	 * @param dnis Array de DNIs de los clientes a consultar
	 * @throws SQLException Si ocurre un error en la base de datos
	 */
	public void consultarClientesPorDNI(String[] dnis) throws SQLException {
		// ValidaciÃ³n de entrada
		if (dnis == null || dnis.length == 0) {
			System.out.println("  No se proporcionaron DNIs para consultar.");
			return;
		}

		System.out.println("=== ACTIVIDAD 4.5 - CONSULTA DE CLIENTES CON PREPARED STATEMENT ===");
		System.out.println("  Consultando " + dnis.length + " cliente(s) individualmente...");
		System.out.println();

		// Sentencia SQL preparada - La consulta SELECT con parÃ¡metro ?
		// El ? serÃ¡ reemplazado por cada DNI especÃ­fico en cada iteraciÃ³n
		String sqlConsulta = "SELECT * FROM CLIENTES WHERE DNI = ?";
		System.out.println("  Consulta SQL: " + sqlConsulta);
		System.out.println();

		// Crear la sentencia preparada UNA SOLA VEZ fuera del bucle
		// Esto es mÃ¡s eficiente que crear una nueva PreparedStatement en cada iteraciÃ³n
		try (PreparedStatement pstmt = connection.prepareStatement(sqlConsulta)) {
			
			// Contadores para estadÃ­sticas
			int clientesEncontrados = 0;
			int clientesNoEncontrados = 0;

			// Iterar sobre cada DNI proporcionado
			for (int i = 0; i < dnis.length; i++) {
				String dniActual = dnis[i];
				
				System.out.println("  Consulta " + (i + 1) + "/" + dnis.length + " - DNI: " + dniActual);
				
				// Validar que el DNI no sea nulo o vacÃ­o
				if (dniActual == null || dniActual.trim().isEmpty()) {
					System.out.println("      DNI invÃ¡lido (nulo o vacÃ­o) - Saltando...");
					clientesNoEncontrados++;
					System.out.println();
					continue;
				}

				// Establecer el parÃ¡metro ? con el DNI actual
				// setString(1, valor) significa: reemplazar el primer ? con 'valor'
				pstmt.setString(1, dniActual.trim());

				// Ejecutar la consulta preparada
				// Como consultamos por clave primaria, esperamos 0 o 1 fila mÃ¡ximo
				try (ResultSet rs = pstmt.executeQuery()) {
					
					// Verificar si se encontrÃ³ el cliente
					if (rs.next()) {
						// Cliente encontrado - extraer datos del ResultSet
						String dni = rs.getString("DNI");
						String apellidos = rs.getString("APELLIDOS");
						
						// Manejar valores NULL en la columna CP
						int cp = rs.getInt("CP");
						String cpTexto = rs.wasNull() ? "null" : String.valueOf(cp);
						
						// Mostrar los datos del cliente encontrado
						System.out.println("     Cliente encontrado:");
						System.out.println("      - DNI: " + dni);
						System.out.println("      - Apellidos: " + apellidos);
						System.out.println("      - CP: " + cpTexto);
						
						clientesEncontrados++;
					} else {
						// No se encontrÃ³ ningÃºn cliente con ese DNI
						System.out.println("     Cliente no encontrado en la base de datos");
						clientesNoEncontrados++;
					}
				}
				
				System.out.println(); // LÃ­nea en blanco para separar resultados
			}

			// Mostrar resumen estadÃ­stico final
			mostrarResumenConsulta(clientesEncontrados, clientesNoEncontrados, dnis.length);
		}
	}

	/**
	 * MÃ©todo sobrecargado que consulta TODOS los clientes de la tabla.
	 * Primero obtiene todos los DNIs y luego los consulta individualmente.
	 */
	public void consultarTodosLosClientesIndividualmente() throws SQLException {
		System.out.println("=== ACTIVIDAD 4.5 - CONSULTA DE TODOS LOS CLIENTES ===");
		System.out.println("Obteniendo lista de todos los DNIs...");

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
			System.out.println("  No hay clientes en la tabla CLIENTES.");
			return;
		}

		System.out.println("Se encontraron " + listaDnis.size() + " cliente(s) en total.");
		System.out.println();

		// Convertir List a Array y llamar al mÃ©todo principal
		String[] arrayDnis = listaDnis.toArray(new String[0]);
		consultarClientesPorDNI(arrayDnis);
	}

	/**
	 * MÃ©todo auxiliar para mostrar el resumen estadÃ­stico de la consulta.
	 * 
	 * @param encontrados NÃºmero de clientes encontrados
	 * @param noEncontrados NÃºmero de clientes no encontrados
	 * @param total Total de consultas realizadas
	 */
	private void mostrarResumenConsulta(int encontrados, int noEncontrados, int total) {
		System.out.println(" RESUMEN DE CONSULTAS:");
		System.out.println("    Total de consultas realizadas: " + total);
		System.out.println("    Clientes encontrados: " + encontrados);
		System.out.println("    Clientes no encontrados: " + noEncontrados);
		
		if (total > 0) {
			double porcentajeExito = (encontrados * 100.0) / total;
			System.out.println("    Porcentaje de Ã©xito: " + String.format("%.1f%%", porcentajeExito));
		}
		
		System.out.println();
		System.out.println("  VENTAJAS DE LAS SENTENCIAS PREPARADAS:");
		System.out.println("    ProtecciÃ³n contra inyecciÃ³n SQL");
		System.out.println("    Mejor rendimiento al reutilizar la consulta compilada");
		System.out.println("    CÃ³digo mÃ¡s limpio y mantenible");
		System.out.println("    Manejo automÃ¡tico de tipos de datos y caracteres especiales");
		System.out.println("=================================================================");
	}

	/**
	 * MÃ©todo de demostraciÃ³n que compara diferentes enfoques de consulta.
	 * Muestra las diferencias entre Statement normal vs PreparedStatement.
	 */
	public void demostrarVentajasPreparedStatement() throws SQLException {
		System.out.println("=== DEMOSTRACIÃ“N: PREPARED STATEMENT VS STATEMENT NORMAL ===");
		
		String[] dnisPrueba = {"78901234X", "89012345E"};
		
		System.out.println("ENFOQUE INSEGURO - Statement Normal (NO recomendado):");
		System.out.println("   String sql = \"SELECT * FROM CLIENTES WHERE DNI = '\" + dni + \"'\";");
		System.out.println("   Vulnerable a inyecciÃ³n SQL");
		System.out.println("   Menos eficiente (recompila cada vez)");
		System.out.println();
		
		System.out.println("ENFOQUE SEGURO - PreparedStatement (RECOMENDADO):");
		System.out.println("   String sql = \"SELECT * FROM CLIENTES WHERE DNI = ?\";");
		System.out.println("   Protegido contra inyecciÃ³n SQL");
		System.out.println("   MÃ¡s eficiente (precompilado)");
		System.out.println();
		
		// Demostrar el uso correcto con PreparedStatement
		String sql = "SELECT * FROM CLIENTES WHERE DNI = ?";
		try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
			
			for (String dni : dnisPrueba) {
				System.out.println("Consultando DNI: " + dni);
				
				// Forma segura: usar setString para evitar inyecciÃ³n SQL
				pstmt.setString(1, dni);
				
				try (ResultSet rs = pstmt.executeQuery()) {
					if (rs.next()) {
						System.out.println("   Cliente: " + rs.getString("APELLIDOS"));
					} else {
						System.out.println("   No encontrado");
					}
				}
			}
		}
		
		System.out.println("\nBUENAS PRÃCTICAS IMPLEMENTADAS:");
		System.out.println("   Usar try-with-resources para gestiÃ³n automÃ¡tica de recursos");
		System.out.println("   Una sola PreparedStatement reutilizada mÃºltiples veces");
		System.out.println("   ValidaciÃ³n de datos de entrada");
		System.out.println("   Manejo adecuado de valores NULL");
		System.out.println("   Comentarios claros explicando cada paso");
		System.out.println("===================================================");
	}

	/**
	 * =================== ACTIVIDAD 4.6 ===================
	 * MÃ©todos para gestiÃ³n de la tabla COMPANIES y operaciones batch.
	 */

	/**
	 * ACTIVIDAD 4.6 - MÃ©todo 1: Crear tabla COMPANIES.
	 * Crea la tabla COMPANIES con los campos especificados si no existe.
	 * 
	 * Estructura de la tabla:
	 * - CIF: VARCHAR(9) PRIMARY KEY - CÃ³digo de identificaciÃ³n fiscal (8 nÃºmeros + 1 letra)
	 * - NOMBRE: VARCHAR(255) NOT NULL - Nombre de la compaÃ±Ã­a
	 * - SECTOR: VARCHAR(100) NOT NULL - Sector al que se dedica la compaÃ±Ã­a
	 * 
	 * @throws SQLException Si ocurre un error al crear la tabla
	 */
	public void crearTablaCompanies() throws SQLException {
		System.out.println("=== ACTIVIDAD 4.6 - CreaciÃ³n de Tabla COMPANIES ===");
		
		// SQL para crear la tabla COMPANIES
		String sqlCrearTabla = """
			CREATE TABLE IF NOT EXISTS COMPANIES (
				CIF VARCHAR(9) PRIMARY KEY COMMENT 'CÃ³digo de IdentificaciÃ³n Fiscal (8 nÃºmeros + 1 letra)',
				NOMBRE VARCHAR(255) NOT NULL COMMENT 'Nombre de la compaÃ±Ã­a',
				SECTOR VARCHAR(100) NOT NULL COMMENT 'Sector al que se dedica la compaÃ±Ã­a',
				CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Fecha de creaciÃ³n del registro'
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
			  COMMENT='Tabla que almacena informaciÃ³n de compaÃ±Ã­as'
		""";
		
		try (java.sql.Statement stmt = connection.createStatement()) {
			System.out.println("Creando tabla COMPANIES...");
			stmt.execute(sqlCrearTabla);
			System.out.println("Tabla COMPANIES creada exitosamente (o ya existÃ­a)");
			
			// Mostrar estructura de la tabla
			mostrarEstructuraTabla("COMPANIES");
			
		} catch (SQLException e) {
			System.err.println("Error al crear la tabla COMPANIES: " + e.getMessage());
			throw e;
		}
		
		System.out.println("=============================================");
	}

	/**
	 * ACTIVIDAD 4.6 - MÃ©todo 2: Insertar compaÃ±Ã­as en modo batch con transacciÃ³n controlada.
	 * 
	 * Este mÃ©todo implementa las mejores prÃ¡cticas para inserciones masivas:
	 * - Uso de PreparedStatement para seguridad contra inyecciÃ³n SQL
	 * - Operaciones batch para mejor rendimiento
	 * - Transacciones controladas con commit/rollback automÃ¡tico
	 * - ValidaciÃ³n de datos antes de la inserciÃ³n
	 * - Manejo robusto de errores
	 * - VerificaciÃ³n de duplicados antes de insertar
	 * 
	 * @param companies Lista de compaÃ±Ã­as a insertar
	 * @return NÃºmero de compaÃ±Ã­as insertadas exitosamente
	 * @throws SQLException Si ocurre un error irrecuperable
	 */
	public int insertarCompaniesEnBatch(List<Company> companies) throws SQLException {
		// ValidaciÃ³n inicial
		if (companies == null || companies.isEmpty()) {
			System.out.println("No se proporcionaron compaÃ±Ã­as para insertar.");
			return 0;
		}

		System.out.println("=== ACTIVIDAD 4.6 - InserciÃ³n Batch de CompaÃ±Ã­as ===");
		System.out.println("Preparando inserciÃ³n de " + companies.size() + " compaÃ±Ã­a(s)...");

		// Primero limpiar tabla para evitar duplicados en las pruebas
		limpiarTablaCompaniesParaPruebas();

		// SQL de inserciÃ³n con parÃ¡metros
		String sqlInsert = "INSERT INTO COMPANIES (CIF, NOMBRE, SECTOR) VALUES (?, ?, ?)";
		
		// Variables para estadÃ­sticas
		int companiesValidadas = 0;
		int companiesInsertadas = 0;
		List<String> erroresValidacion = new ArrayList<>();

		// Guardar el estado original del autoCommit
		boolean autoCommitOriginal = connection.getAutoCommit();
		
		try {
			// Desactivar autoCommit para control manual de transacciones
			connection.setAutoCommit(false);
			System.out.println("TransacciÃ³n iniciada (autoCommit = false)");

			try (PreparedStatement pstmt = connection.prepareStatement(sqlInsert)) {
				
				// Validar y preparar cada compaÃ±Ã­a para el batch
				for (int i = 0; i < companies.size(); i++) {
					Company company = companies.get(i);
					
					System.out.println("Validando compaÃ±Ã­a " + (i + 1) + "/" + companies.size() + ": " + 
									 (company != null ? company.getCif() : "null"));
					
					// ValidaciÃ³n completa de la compaÃ±Ã­a
					String errorValidacion = validarCompanyCompleta(company);
					if (errorValidacion != null) {
						erroresValidacion.add("CompaÃ±Ã­a " + (i + 1) + ": " + errorValidacion);
						System.out.println("   ERROR: " + errorValidacion);
						continue;
					}
					
					// Agregar al batch si la validaciÃ³n fue exitosa
					pstmt.setString(1, company.getCif().toUpperCase().trim());
					pstmt.setString(2, company.getNombre().trim());
					pstmt.setString(3, company.getSector().trim());
					pstmt.addBatch();
					
					companiesValidadas++;
					System.out.println("   OK: CompaÃ±Ã­a vÃ¡lida agregada al batch");
				}

				// Verificar si hay compaÃ±Ã­as vÃ¡lidas para insertar
				if (companiesValidadas == 0) {
					System.out.println("No hay compaÃ±Ã­as vÃ¡lidas para insertar");
					connection.rollback();
					return 0;
				}

				System.out.println("\nEjecutando batch de " + companiesValidadas + " compaÃ±Ã­a(s)...");
				
				// Ejecutar el batch
				int[] resultados = pstmt.executeBatch();
				
				// Analizar resultados del batch
				for (int i = 0; i < resultados.length; i++) {
					if (resultados[i] > 0 || resultados[i] == java.sql.Statement.SUCCESS_NO_INFO) {
						companiesInsertadas++;
					}
				}

				// Si todo saliÃ³ bien, confirmar la transacciÃ³n
				connection.commit();
				System.out.println("TRANSACCIÃ“N CONFIRMADA (COMMIT)");
				
				// Mostrar estadÃ­sticas finales
				mostrarEstadisticasInsercion(companies.size(), companiesValidadas, 
										   companiesInsertadas, erroresValidacion);

			} catch (SQLException e) {
				// Error durante la ejecuciÃ³n del batch - hacer rollback
				System.err.println("Error durante la inserciÃ³n batch: " + e.getMessage());
				connection.rollback();
				System.err.println("TRANSACCIÃ“N REVERTIDA (ROLLBACK)");
				throw e;
			}

		} finally {
			// Restaurar el estado original del autoCommit
			connection.setAutoCommit(autoCommitOriginal);
			System.out.println("AutoCommit restaurado a: " + autoCommitOriginal);
		}

		return companiesInsertadas;
	}

	/**
	 * Limpia la tabla COMPANIES para pruebas (elimina datos de pruebas anteriores).
	 */
	private void limpiarTablaCompaniesParaPruebas() throws SQLException {
		String sqlLimpiar = "DELETE FROM COMPANIES WHERE CIF IN ('12345678A','87654321B','11223344C','55667788D','99887766E')";
		
		try (java.sql.Statement stmt = connection.createStatement()) {
			int eliminados = stmt.executeUpdate(sqlLimpiar);
			if (eliminados > 0) {
				System.out.println("Eliminados " + eliminados + " registros de pruebas anteriores");
			}
		}
	}

	/**
	 * ValidaciÃ³n completa incluyendo verificaciÃ³n de duplicados.
	 */
	private String validarCompanyCompleta(Company company) throws SQLException {
		// ValidaciÃ³n bÃ¡sica
		String errorBasico = validarCompany(company);
		if (errorBasico != null) {
			return errorBasico;
		}

		// Verificar si ya existe el CIF
		if (existeCifEnBD(company.getCif())) {
			return "CIF ya existe en la base de datos: " + company.getCif();
		}

		return null; // ValidaciÃ³n exitosa
	}

	/**
	 * Verifica si un CIF ya existe en la base de datos.
	 */
	private boolean existeCifEnBD(String cif) throws SQLException {
		String sql = "SELECT COUNT(*) FROM COMPANIES WHERE CIF = ?";
		
		try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
			pstmt.setString(1, cif.toUpperCase().trim());
			
			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					return rs.getInt(1) > 0;
				}
			}
		}
		
		return false;
	}

	/**
	 * MÃ©todo auxiliar para validar una compaÃ±Ã­a antes de la inserciÃ³n.
	 * 
	 * @param company CompaÃ±Ã­a a validar
	 * @return null si es vÃ¡lida, mensaje de error si no es vÃ¡lida
	 */
	private String validarCompany(Company company) {
		if (company == null) {
			return "CompaÃ±Ã­a es null";
		}
		
		if (!company.isComplete()) {
			return "CompaÃ±Ã­a incompleta (faltan campos obligatorios)";
		}
		
		if (!company.isValidCif()) {
			return "CIF invÃ¡lido (debe tener 8 nÃºmeros + 1 letra): " + company.getCif();
		}
		
		if (company.getNombre().length() > 255) {
			return "Nombre demasiado largo (mÃ¡ximo 255 caracteres)";
		}
		
		if (company.getSector().length() > 100) {
			return "Sector demasiado largo (mÃ¡ximo 100 caracteres)";
		}
		
		return null; // ValidaciÃ³n exitosa
	}

	/**
	 * MÃ©todo auxiliar para mostrar la estructura de una tabla.
	 * 
	 * @param nombreTabla Nombre de la tabla a describir
	 */
	private void mostrarEstructuraTabla(String nombreTabla) throws SQLException {
		String sqlDescribe = "DESCRIBE " + nombreTabla;
		
		System.out.println("\nEstructura de la tabla " + nombreTabla + ":");
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
	 * MÃ©todo auxiliar para mostrar estadÃ­sticas de la inserciÃ³n.
	 */
	private void mostrarEstadisticasInsercion(int total, int validadas, int insertadas, 
											List<String> errores) {
		System.out.println("\nESTADÃSTICAS DE INSERCIÃ“N:");
		System.out.println("   Total de compaÃ±Ã­as procesadas: " + total);
		System.out.println("   CompaÃ±Ã­as validadas: " + validadas);
		System.out.println("   CompaÃ±Ã­as insertadas: " + insertadas);
		System.out.println("   CompaÃ±Ã­as rechazadas: " + (total - validadas));
		
		if (total > 0) {
			double porcentajeExito = (insertadas * 100.0) / total;
			System.out.println("   Porcentaje de Ã©xito: " + String.format("%.1f%%", porcentajeExito));
		}
		
		if (!errores.isEmpty()) {
			System.out.println("\nERRORES DE VALIDACIÃ“N:");
			for (String error : errores) {
				System.out.println("   - " + error);
			}
		}
		
		System.out.println("\nVENTAJAS DEL MÃ‰TODO IMPLEMENTADO:");
		System.out.println("   Seguridad: PreparedStatement previene inyecciÃ³n SQL");
		System.out.println("   Rendimiento: Operaciones batch para inserciones masivas");
		System.out.println("   Robustez: Transacciones controladas con rollback automÃ¡tico");
		System.out.println("   ValidaciÃ³n: VerificaciÃ³n completa de datos antes de insertar");
		System.out.println("   InformaciÃ³n: EstadÃ­sticas detalladas del proceso");
		System.out.println("=======================================================");
	}

	/**
	 * MÃ©todo auxiliar para consultar y mostrar las compaÃ±Ã­as insertadas.
	 * 
	 * @throws SQLException Si ocurre un error en la consulta
	 */
	public void mostrarCompaniesInsertadas() throws SQLException {
		System.out.println("=== COMPAÃ‘ÃAS EN LA BASE DE DATOS ===");
		
		String sql = "SELECT CIF, NOMBRE, SECTOR, CREATED_AT FROM COMPANIES ORDER BY CREATED_AT DESC";
		
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sql)) {
			
			System.out.println("CIF\t\tNOMBRE\t\t\tSECTOR\t\tFECHA CREACIÃ“N");
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
				System.out.println("   No hay compaÃ±Ã­as registradas.");
			} else {
				System.out.println("\nTotal de compaÃ±Ã­as: " + contador);
			}
		}
		
		System.out.println("====================================");
	}

	/**
	 * MÃ©todo de demostraciÃ³n que muestra diferentes escenarios de batch insert.
	 * Incluye casos de Ã©xito, error y recuperaciÃ³n.
	 */
	public void demostrarEscenariosCompanies() throws SQLException {
		System.out.println("=== DEMOSTRACIÃ“N: ESCENARIOS DE BATCH INSERT ===");
		
		// Escenario 1: Batch completamente vÃ¡lido
		System.out.println("Escenario 1: Todas las compaÃ±Ã­as son vÃ¡lidas");
		List<Company> companiesValidas = Arrays.asList(
			new Company("10101010A", "Empresa Ejemplo 1", "Retail"),
			new Company("20202020B", "Empresa Ejemplo 2", "Servicios"),
			new Company("30303030C", "Empresa Ejemplo 3", "Industrial")
		);
		insertarCompaniesEnBatch(companiesValidas);
		
		// Escenario 2: Batch con errores mezclados
		System.out.println("\nEscenario 2: Mezcla de vÃ¡lidas e invÃ¡lidas");
		List<Company> companiesMixtas = Arrays.asList(
			new Company("40404040D", "Empresa VÃ¡lida", "TecnologÃ­a"),
			new Company("INVALID02", "CIF Malo", "Tech"), // CIF invÃ¡lido
			new Company("50505050E", "Otra VÃ¡lida", "ConsultorÃ­a")
		);
		insertarCompaniesEnBatch(companiesMixtas);
		
		// Escenario 3: Intento de duplicados (deberÃ­a fallar por clave primaria)
		System.out.println("\nEscenario 3: Intento de insertar duplicados");
		List<Company> companiesDuplicadas = Arrays.asList(
			new Company("10101010A", "Empresa Duplicada", "Retail") // CIF ya existe
		);
		
		try {
			insertarCompaniesEnBatch(companiesDuplicadas);
		} catch (SQLException e) {
			System.out.println("Error esperado por duplicado: " + e.getMessage());
			System.out.println("El sistema maneja correctamente los duplicados");
		}
		
		System.out.println("\nEstado final de la tabla:");
		mostrarCompaniesInsertadas();
		
		System.out.println("==============================================");
	}

	/**
	 * =================== ACTIVIDAD 4.7 ===================
	 * MÃ©todos para crear y utilizar funciones almacenadas en MySQL.
	 */

	/**
	 * ACTIVIDAD 4.7 - MÃ©todo 1: Crear funciÃ³n almacenada en MySQL.
	 * 
	 * Crea una funciÃ³n llamada 'obtener_apellidos_cliente' que:
	 * - Recibe un parÃ¡metro DNI (VARCHAR(20))
	 * - Devuelve los apellidos del cliente (VARCHAR(255))
	 * - Retorna NULL si no se encuentra el cliente
	 * 
	 * SINTAXIS de funciÃ³n almacenada en MySQL:
	 * CREATE FUNCTION nombre_funcion(parametros) RETURNS tipo_retorno
	 * [DETERMINISTIC | NOT DETERMINISTIC]
	 * [READS SQL DATA | MODIFIES SQL DATA | NO SQL | CONTAINS SQL]
	 * BEGIN
	 *   -- lÃ³gica de la funciÃ³n
	 *   RETURN valor;
	 * END
	 * 
	 * @throws SQLException Si ocurre un error al crear la funciÃ³n
	 */
	public void crearFuncionAlmacenadaObtenerApellidos() throws SQLException {
		System.out.println("=== ACTIVIDAD 4.7 - CreaciÃ³n de FunciÃ³n Almacenada ===");
		
		// Primero, eliminar la funciÃ³n si ya existe (para evitar errores)
		String sqlEliminarFuncion = "DROP FUNCTION IF EXISTS obtener_apellidos_cliente";
		
		// SQL para crear la funciÃ³n almacenada
		String sqlCrearFuncion = """
			CREATE FUNCTION obtener_apellidos_cliente(dni_param VARCHAR(20))
			RETURNS VARCHAR(255)
			READS SQL DATA
			DETERMINISTIC
			BEGIN
				DECLARE apellidos_resultado VARCHAR(255);
				
				-- Consultar los apellidos del cliente por su DNI
				SELECT APELLIDOS INTO apellidos_resultado
				FROM CLIENTES 
				WHERE DNI = dni_param;
				
				-- Devolver los apellidos encontrados (o NULL si no existe)
				RETURN apellidos_resultado;
			END
		""";
		
		try (java.sql.Statement stmt = connection.createStatement()) {
			
			System.out.println("Eliminando funciÃ³n existente (si la hay)...");
			stmt.execute(sqlEliminarFuncion);
			System.out.println("FunciÃ³n anterior eliminada (o no existÃ­a)");
			
			System.out.println("Creando funciÃ³n almacenada 'obtener_apellidos_cliente'...");
			stmt.execute(sqlCrearFuncion);
			System.out.println("FunciÃ³n almacenada creada exitosamente");
			
			// Mostrar informaciÃ³n sobre la funciÃ³n creada
			mostrarInformacionFuncion("obtener_apellidos_cliente");
			
		} catch (SQLException e) {
			System.err.println("Error al crear la funciÃ³n almacenada: " + e.getMessage());
			System.err.println("AsegÃºrate de que tu usuario MySQL tenga permisos para crear funciones");
			System.err.println("Ejecuta: SET GLOBAL log_bin_trust_function_creators = 1;");
			throw e;
		}
		
		System.out.println("===================================================");
	}

	/**
	 * ACTIVIDAD 4.7 - MÃ©todo 2: Llamar funciÃ³n almacenada desde Java.
	 * 
	 * Demuestra cÃ³mo llamar una funciÃ³n almacenada MySQL desde Java usando JDBC.
	 * Las funciones almacenadas se llaman dentro de consultas SELECT, a diferencia 
	 * de los procedimientos almacenados que se llaman con CALL.
	 * 
	 * @param dni DNI del cliente cuyos apellidos queremos obtener
	 * @return Apellidos del cliente, o null si no se encuentra
	 * @throws SQLException Si ocurre un error en la consulta
	 */
	public String llamarFuncionObtenerApellidos(String dni) throws SQLException {
		// ValidaciÃ³n de entrada
		if (dni == null || dni.trim().isEmpty()) {
			throw new IllegalArgumentException("El DNI no puede ser nulo o vacÃ­o");
		}
		
		System.out.println(" Llamando funciÃ³n almacenada para DNI: " + dni);
		
		// SQL para llamar la funciÃ³n almacenada
		// NOTA: Las funciones se llaman dentro de SELECT, no con CALL
		String sqlLlamarFuncion = "SELECT obtener_apellidos_cliente(?) AS apellidos";
		
		try (PreparedStatement pstmt = connection.prepareStatement(sqlLlamarFuncion)) {
			
			// Establecer el parÃ¡metro DNI
			pstmt.setString(1, dni.trim());
			
			// Ejecutar la consulta
			try (ResultSet rs = pstmt.executeQuery()) {
				
				if (rs.next()) {
					String apellidos = rs.getString("apellidos");
					
					if (apellidos != null) {
						System.out.println("âœ… Apellidos encontrados: " + apellidos);
						return apellidos;
					} else {
						System.out.println("âŒ Cliente no encontrado para DNI: " + dni);
						return null;
					}
				} else {
					System.out.println("âŒ Error: La funciÃ³n no devolviÃ³ ningÃºn resultado");
					return null;
				}
			}
		} catch (SQLException e) {
			System.err.println("âŒ Error al llamar la funciÃ³n almacenada: " + e.getMessage());
			throw e;
		}
	}

	/**
	 * ACTIVIDAD 4.7 - MÃ©todo 3: Demostrar mÃºltiples llamadas a la funciÃ³n.
	 * 
	 * Demuestra el uso de la funciÃ³n almacenada con varios DNIs diferentes,
	 * incluyendo casos de Ã©xito y casos donde el cliente no existe.
	 * 
	 * @param dnis Array de DNIs para probar
	 * @throws SQLException Si ocurre un error en las consultas
	 */
	public void demostrarFuncionAlmacenada(String[] dnis) throws SQLException {
		System.out.println("=== ACTIVIDAD 4.7 - DemostraciÃ³n de FunciÃ³n Almacenada ===");
		
		if (dnis == null || dnis.length == 0) {
			System.out.println("âš ï¸ No se proporcionaron DNIs para probar");
			return;
		}
		
		System.out.println("ðŸ§ª Probando funciÃ³n 'obtener_apellidos_cliente' con " + dnis.length + " DNI(s):");
		System.out.println();
		
		int encontrados = 0;
		int noEncontrados = 0;
		
		// Probar la funciÃ³n con cada DNI
		for (int i = 0; i < dnis.length; i++) {
			String dni = dnis[i];
			
			System.out.println("ðŸ“‹ Prueba " + (i + 1) + "/" + dnis.length + ":");
			System.out.println("   DNI: " + (dni != null ? dni : "null"));
			
			try {
				String apellidos = llamarFuncionObtenerApellidos(dni);
				
				if (apellidos != null) {
					System.out.println("   âœ… Resultado: " + apellidos);
					encontrados++;
				} else {
					System.out.println("   âŒ Resultado: Cliente no encontrado");
					noEncontrados++;
				}
				
			} catch (Exception e) {
				System.out.println("   âŒ Error: " + e.getMessage());
				noEncontrados++;
			}
			
			System.out.println();
		}
		
		// Mostrar estadÃ­sticas
		System.out.println("ðŸ“Š ESTADÃSTICAS DE PRUEBAS:");
		System.out.println("   ðŸŽ¯ Total de pruebas: " + dnis.length);
		System.out.println("   âœ… Clientes encontrados: " + encontrados);
		System.out.println("   âŒ Clientes no encontrados: " + noEncontrados);
		
		if (dnis.length > 0) {
			double porcentajeExito = (encontrados * 100.0) / dnis.length;
			System.out.println("   ðŸ“ˆ Porcentaje de Ã©xito: " + String.format("%.1f%%", porcentajeExito));
		}
		
		System.out.println();
		System.out.println("ðŸ’¡ VENTAJAS DE LAS FUNCIONES ALMACENADAS:");
		System.out.println("   ðŸš€ Rendimiento: LÃ³gica ejecutada en el servidor de BD");
		System.out.println("   ðŸ”’ Seguridad: LÃ³gica encapsulada y controlada");
		System.out.println("   ðŸŽ¯ ReutilizaciÃ³n: Una funciÃ³n, mÃºltiples aplicaciones");
		System.out.println("   ðŸ“Š Mantenibilidad: Cambios centralizados en la BD");
		System.out.println("   ðŸ”„ Consistencia: Misma lÃ³gica para todos los clientes");
		
		System.out.println("=======================================================");
	}

	/**
	 * MÃ©todo auxiliar para mostrar informaciÃ³n sobre la funciÃ³n almacenada creada.
	 * 
	 * @param nombreFuncion Nombre de la funciÃ³n a consultar
	 * @throws SQLException Si ocurre un error en la consulta
	 */
	private void mostrarInformacionFuncion(String nombreFuncion) throws SQLException {
		System.out.println("\nðŸ“‹ InformaciÃ³n de la funciÃ³n almacenada:");
		
		// Consultar informaciÃ³n de la funciÃ³n desde INFORMATION_SCHEMA
		String sqlInfoFuncion = """
			SELECT 
				ROUTINE_NAME as 'Nombre',
				ROUTINE_TYPE as 'Tipo',
				DTD_IDENTIFIER as 'Tipo_Retorno',
				ROUTINE_BODY as 'Lenguaje',
				IS_DETERMINISTIC as 'Deterministica',
				SQL_DATA_ACCESS as 'Acceso_Datos'
			FROM INFORMATION_SCHEMA.ROUTINES 
			WHERE ROUTINE_SCHEMA = DATABASE() 
			AND ROUTINE_NAME = ?
		""";
		
		try (PreparedStatement pstmt = connection.prepareStatement(sqlInfoFuncion)) {
			pstmt.setString(1, nombreFuncion);
			
			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					System.out.println("   ðŸ·ï¸  Nombre: " + rs.getString("Nombre"));
					System.out.println("   ðŸ“ Tipo: " + rs.getString("Tipo"));
					System.out.println("   ðŸ”„ Tipo de retorno: " + rs.getString("Tipo_Retorno"));
					System.out.println("   ðŸ’» Lenguaje: " + rs.getString("Lenguaje"));
					System.out.println("   ðŸŽ¯ DeterminÃ­stica: " + rs.getString("Deterministica"));
					System.out.println("   ðŸ“Š Acceso a datos: " + rs.getString("Acceso_Datos"));
				} else {
					System.out.println("   âš ï¸  No se encontrÃ³ informaciÃ³n de la funciÃ³n");
				}
			}
		}
	}

	/**
	 * MÃ©todo auxiliar que compara el rendimiento entre funciÃ³n almacenada y consulta directa.
	 * Demuestra cuÃ¡ndo es ventajoso usar funciones almacenadas.
	 * 
	 * @param dni DNI para la comparaciÃ³n
	 * @throws SQLException Si ocurre un error en las consultas
	 */
	public void compararRendimientoFuncionVsConsulta(String dni) throws SQLException {
		System.out.println("=== COMPARACIÃ“N: FUNCIÃ“N ALMACENADA vs CONSULTA DIRECTA ===");
		System.out.println("ðŸ§ª Probando con DNI: " + dni);
		System.out.println();
		
		// MÃ©todo 1: Usando funciÃ³n almacenada
		long inicioFuncion = System.currentTimeMillis();
		String resultadoFuncion = null;
		
		try {
			resultadoFuncion = llamarFuncionObtenerApellidos(dni);
		} catch (Exception e) {
			System.out.println("âŒ Error en funciÃ³n almacenada: " + e.getMessage());
		}
		
		long tiempoFuncion = System.currentTimeMillis() - inicioFuncion;
		
		// MÃ©todo 2: Usando consulta directa
		long inicioConsulta = System.currentTimeMillis();
		String resultadoConsulta = null;
		
		String sqlConsultaDirecta = "SELECT APELLIDOS FROM CLIENTES WHERE DNI = ?";
		try (PreparedStatement pstmt = connection.prepareStatement(sqlConsultaDirecta)) {
			pstmt.setString(1, dni);
			
			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					resultadoConsulta = rs.getString("APELLIDOS");
				}
			}
		}
		
		long tiempoConsulta = System.currentTimeMillis() - inicioConsulta;
		
		// Mostrar resultados de la comparaciÃ³n
		System.out.println("ðŸ“Š RESULTADOS DE LA COMPARACIÃ“N:");
		System.out.println();
		System.out.println("ðŸ”§ FunciÃ³n Almacenada:");
		System.out.println("   â±ï¸  Tiempo: " + tiempoFuncion + " ms");
		System.out.println("   ðŸ“‹ Resultado: " + (resultadoFuncion != null ? resultadoFuncion : "No encontrado"));
		System.out.println();
		System.out.println("ðŸ“Š Consulta Directa:");
		System.out.println("   â±ï¸  Tiempo: " + tiempoConsulta + " ms");
		System.out.println("   ðŸ“‹ Resultado: " + (resultadoConsulta != null ? resultadoConsulta : "No encontrado"));
		System.out.println();
		
		// Verificar consistencia
		boolean resultadosIguales = (resultadoFuncion == null && resultadoConsulta == null) ||
									(resultadoFuncion != null && resultadoFuncion.equals(resultadoConsulta));
		
		System.out.println("âœ… Consistencia de resultados: " + (resultadosIguales ? "CORRECTA" : "ERROR"));
		
		if (tiempoFuncion < tiempoConsulta) {
			System.out.println("ðŸ† La funciÃ³n almacenada fue mÃ¡s rÃ¡pida");
		} else if (tiempoConsulta < tiempoFuncion) {
			System.out.println("ðŸ† La consulta directa fue mÃ¡s rÃ¡pida");
		} else {
			System.out.println("ðŸ¤ Ambos mÃ©todos tuvieron el mismo rendimiento");
		}
		
		System.out.println("==================================================");
	}

	/**
	 * MÃ©todo adicional que demuestra tÃ©cnicas avanzadas con funciones almacenadas.
	 * Incluye manejo de errores, optimizaciÃ³n y buenas prÃ¡cticas.
	 */
	public void demostrarTecnicasAvanzadasFunciones() throws SQLException {
		System.out.println("=== TÃ‰CNICAS AVANZADAS CON FUNCIONES ALMACENADAS ===");
		
		// 1. Crear una funciÃ³n mÃ¡s compleja que valide el formato del DNI
		crearFuncionValidarDNI();
		
		// 2. Crear una funciÃ³n que combine mÃºltiples campos
		crearFuncionInformacionCompleta();
		
		// 3. Demostrar uso de mÃºltiples funciones en una consulta
		demostrarConsultaCombinada();
		
		System.out.println("==============================================================");
	}

	/**
	 * Crea una funciÃ³n almacenada que valida si un DNI tiene formato correcto.
	 */
	private void crearFuncionValidarDNI() throws SQLException {
		System.out.println("ðŸ”§ Creando funciÃ³n de validaciÃ³n de DNI...");
		
		String sqlEliminar = "DROP FUNCTION IF EXISTS validar_dni_formato";
		String sqlCrear = """
			CREATE FUNCTION validar_dni_formato(dni_param VARCHAR(20))
			RETURNS BOOLEAN
			READS SQL DATA
			DETERMINISTIC
			BEGIN
				-- Verificar que el DNI no sea NULL y tenga longitud correcta
				IF dni_param IS NULL OR LENGTH(dni_param) != 9 THEN
					RETURN FALSE;
				END IF;
				
				-- Verificar que los primeros 8 caracteres sean nÃºmeros
				IF SUBSTRING(dni_param, 1, 8) NOT REGEXP '^[0-9]{8}$' THEN
					RETURN FALSE;
				END IF;
				
				-- Verificar que el Ãºltimo carÃ¡cter sea una letra
				IF SUBSTRING(dni_param, 9, 1) NOT REGEXP '^[A-Za-z]$' THEN
					RETURN FALSE;
				END IF;
				
				RETURN TRUE;
			END
		""";
		
		try (java.sql.Statement stmt = connection.createStatement()) {
			stmt.execute(sqlEliminar);
			stmt.execute(sqlCrear);
			System.out.println("âœ… FunciÃ³n 'validar_dni_formato' creada");
		}
	}

	/**
	 * Crea una funciÃ³n que devuelve informaciÃ³n completa del cliente.
	 */
	private void crearFuncionInformacionCompleta() throws SQLException {
		System.out.println("ðŸ”§ Creando funciÃ³n de informaciÃ³n completa...");
		
		String sqlEliminar = "DROP FUNCTION IF EXISTS obtener_info_cliente";
		String sqlCrear = """
			CREATE FUNCTION obtener_info_cliente(dni_param VARCHAR(20))
			RETURNS VARCHAR(500)
			READS SQL DATA
			DETERMINISTIC
			BEGIN
				DECLARE info_resultado VARCHAR(500);
				DECLARE apellidos_temp VARCHAR(255);
				DECLARE cp_temp INT;
				
				-- Obtener datos del cliente
				SELECT APELLIDOS, CP INTO apellidos_temp, cp_temp
				FROM CLIENTES 
				WHERE DNI = dni_param;
				
				-- Si no se encontrÃ³ el cliente
				IF apellidos_temp IS NULL THEN
					RETURN CONCAT('Cliente con DNI ', dni_param, ' no encontrado');
				END IF;
				
				-- Construir informaciÃ³n completa
				SET info_resultado = CONCAT(
					'DNI: ', dni_param,
					', Apellidos: ', apellidos_temp,
					', CP: ', IFNULL(cp_temp, 'No especificado')
				);
				
				RETURN info_resultado;
			END
		""";
		
		try (java.sql.Statement stmt = connection.createStatement()) {
			stmt.execute(sqlEliminar);
			stmt.execute(sqlCrear);
			System.out.println("âœ… FunciÃ³n 'obtener_info_cliente' creada");
		}
	}

	/**
	 * Demuestra el uso combinado de mÃºltiples funciones en una sola consulta.
	 */
	private void demostrarConsultaCombinada() throws SQLException {
		System.out.println("\nðŸ§ª Demostrando consulta con mÃºltiples funciones:");
		
		String sqlCombinada = """
			SELECT 
				DNI,
				validar_dni_formato(DNI) as DNI_VALIDO,
				obtener_apellidos_cliente(DNI) as APELLIDOS,
				obtener_info_cliente(DNI) as INFO_COMPLETA
			FROM CLIENTES
			LIMIT 3
		""";
		
		try (java.sql.Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sqlCombinada)) {
			
			System.out.println("\nðŸ“Š Resultados de consulta combinada:");
			System.out.println("DNI\t\tVÃLIDO\tAPELLIDOS\tINFO COMPLETA");
			System.out.println("â”€".repeat(80));
			
			while (rs.next()) {
				System.out.printf("%-12s %-8s %-15s %s%n",
					rs.getString("DNI"),
					rs.getBoolean("DNI_VALIDO") ? "SÃ" : "NO",
					rs.getString("APELLIDOS"),
					rs.getString("INFO_COMPLETA")
				);
			}
		}
	}

	/**
	 * ACTIVIDAD 4.8: Navegador interactivo de tabla
	 * Permite navegar por los contenidos de una tabla usando comandos:
	 * - "k": siguiente fila
	 * - "d": fila anterior
	 * - numero: ir a fila específica
	 * - ".": salir
	 */
	public void navegadorInteractivoTabla(String nombreTabla) throws SQLException, java.io.IOException {
		System.out.println("============================================================");
		System.out.println("ACTIVIDAD 4.8: NAVEGADOR INTERACTIVO DE TABLA");
		System.out.println("============================================================");
		System.out.println("Tabla: " + nombreTabla);
		System.out.println();
		System.out.println("COMANDOS DISPONIBLES:");
		System.out.println("  k - Ir a la siguiente fila");
		System.out.println("  d - Ir a la fila anterior");
		System.out.println("  [numero] - Ir a una fila específica");
		System.out.println("  . - Salir del navegador");
		System.out.println("============================================================");
		System.out.println();

		// Crear consulta con ResultSet scrollable
		String sql = "SELECT * FROM " + nombreTabla;
		
		try (java.sql.Statement stmt = connection.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY);
			 ResultSet rs = stmt.executeQuery(sql)) {

			// Verificar que hay datos
			if (!rs.next()) {
				System.out.println("La tabla está vacía.");
				return;
			}

			// Obtener metadata para mostrar nombres de columnas
			java.sql.ResultSetMetaData metaData = rs.getMetaData();
			int numColumnas = metaData.getColumnCount();

			// Mostrar primera fila
			mostrarFilaActual(rs, metaData, numColumnas);

			// Leer comandos del usuario
			java.io.BufferedReader br = new java.io.BufferedReader(
				new java.io.InputStreamReader(System.in)
			);

			while (true) {
				System.out.print("\nComando: ");
				String comando = br.readLine().trim();

				if (comando.equals(".")) {
					System.out.println("Saliendo del navegador...");
					break;
				} 
				else if (comando.equalsIgnoreCase("k")) {
					// Ir a siguiente fila
					if (rs.next()) {
						mostrarFilaActual(rs, metaData, numColumnas);
					} else {
						System.out.println("ERROR: Ya estás en la última fila");
						rs.last(); // Volver a la última fila válida
					}
				} 
				else if (comando.equalsIgnoreCase("d")) {
					// Ir a fila anterior
					if (rs.previous()) {
						mostrarFilaActual(rs, metaData, numColumnas);
					} else {
						System.out.println("ERROR: Ya estás en la primera fila");
						rs.first(); // Volver a la primera fila válida
					}
				} 
				else {
					// Intentar interpretar como número de fila
					try {
						int numeroFila = Integer.parseInt(comando);
						if (numeroFila < 1) {
							System.out.println("ERROR: El número de fila debe ser mayor que 0");
							continue;
						}
						
						if (rs.absolute(numeroFila)) {
							mostrarFilaActual(rs, metaData, numColumnas);
						} else {
							System.out.println("ERROR: La fila " + numeroFila + " no existe");
							// Volver a la posición válida
							if (rs.isAfterLast()) {
								rs.last();
							} else if (rs.isBeforeFirst()) {
								rs.first();
							}
						}
					} catch (NumberFormatException e) {
						System.out.println("ERROR: Comando no reconocido. Usa 'k', 'd', un número, o '.'");
					}
				}
			}
		}
	}

	/**
	 * Método auxiliar para mostrar la fila actual del ResultSet
	 */
	private void mostrarFilaActual(ResultSet rs, java.sql.ResultSetMetaData metaData, int numColumnas) 
			throws SQLException {
		int numeroFila = rs.getRow();
		System.out.println("\n--- Fila " + numeroFila + " ---");
		
		for (int i = 1; i <= numColumnas; i++) {
			String nombreColumna = metaData.getColumnName(i);
			Object valor = rs.getObject(i);
			String valorStr = (valor != null) ? valor.toString() : "NULL";
			
			System.out.println(nombreColumna + ": " + valorStr);
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

