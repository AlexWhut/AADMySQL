Hibernate Project
=================

Resumen
-------
Proyecto Spring Boot que contiene entidades JPA generadas desde una base de datos (POJOs) y configuradas para usarse con MySQL.

Qué hay en el repo
------------------
- `src/main/java/com/acc/datos/hibernate_project/pojos` — entidades generadas / corregidas (Sede, Departamento, Proyecto, Empleado, DatosProfesionales, etc.).
- `src/main/resources/application.properties` — configuración central de Spring Boot (datasource y Hibernate).
- `pom.xml` — perfil Maven `generate-entities` (opt-in) para ejecutar `hbm2java` solo cuando lo pidas.

Compilar
--------
Desde PowerShell (módulo `hibernate_project`):

```powershell
Push-Location 'C:\Users\DAM2_Diurno\Desktop\Github\AADMySQL\hibernate_project\hibernate_project'
.\mvnw.cmd -DskipTests compile
Pop-Location
```

Ejecutar la aplicación
----------------------
Asegúrate de que `src/main/resources/application.properties` contiene la URL, usuario y contraseña correctos para tu base de datos `proyecto_orm`.

Luego:

```powershell
Push-Location 'C:\Users\DAM2_Diurno\Desktop\Github\AADMySQL\hibernate_project\hibernate_project'
.\mvnw.cmd spring-boot:run
Pop-Location
```

Generar entidades (opcional)
---------------------------
El plugin `hibernate-tools-maven` ha sido dejado comentado por defecto. Hay un perfil Maven `generate-entities` para activarlo cuando quieras regenerar POJOs desde la base de datos.

Para usarlo (se necesita acceso JDBC a la BBDD):

```powershell
Push-Location 'C:\Users\DAM2_Diurno\Desktop\Github\AADMySQL\hibernate_project\hibernate_project'
.\mvnw.cmd -Pgenerate-entities clean generate-sources
Pop-Location
```

Notas importantes
-----------------
- He eliminado `hibernate.properties` de `src/main/resources` y consolidado la configuración en `application.properties`. Si ves mensajes sobre `hibernate.properties` al arrancar, ejecuta `mvnw.cmd clean` para asegurarte de que no queda una copia en `target`.
- `DatosProfesionales` ahora usa `@MapsId` para el mapeo de clave primaria compartida (en vez de `@GenericGenerator` deprecated).
- ID de entidades `Sede`, `Departamento` y `Proyecto` están anotados con `@GeneratedValue(strategy = GenerationType.IDENTITY)` y usan tipos `int` en los getters.
- `spring.jpa.hibernate.ddl-auto` está configurado a `validate`. Si tu BBDD no tiene el esquema exacto, el arranque fallará. Para desarrollo temporal puedes cambiar a `update` (no recomendado en producción).

Sugerencias
-----------
- Mantén el plugin `generate-entities` como perfil opt-in para evitar regeneraciones accidentales.
- Versiona `application.properties` con valores no sensibles o usa `application-local.properties`/variables de entorno para credenciales.

Contacto
-------
Si quieres que haga pasos adicionales (crear tests, añadir documentación más detallada o cambiar la estrategia de generación), dime qué prefieres.
