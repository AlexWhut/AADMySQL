package id.morantesbryan.pojos;

/**
 * ACTIVIDAD 4.6 - POJO que representa una compañía.
 * 
 * Esta clase encapsula los datos de una empresa con sus principales atributos:
 * - CIF: Código de Identificación Fiscal (8 números + 1 letra)
 * - NOMBRE: Razón social de la compañía
 * - SECTOR: Sector económico al que pertenece
 * 
 * @author morantesbryan
 * @version 1.0
 */
public class Company {
    
    // Atributos de la compañía
    private String cif;     // CIF: 8 números + 1 letra (ej: "12345678A")
    private String nombre;  // Nombre de la compañía
    private String sector;  // Sector al que se dedica la compañía
    
    /**
     * Constructor por defecto.
     * Necesario para frameworks que requieren instanciación sin parámetros.
     */
    public Company() {
    }
    
    /**
     * Constructor con todos los parámetros.
     * 
     * @param cif CIF de la compañía (8 números + 1 letra)
     * @param nombre Nombre de la compañía
     * @param sector Sector al que pertenece la compañía
     */
    public Company(String cif, String nombre, String sector) {
        this.cif = cif;
        this.nombre = nombre;
        this.sector = sector;
    }
    
    // === GETTERS Y SETTERS ===
    
    /**
     * Obtiene el CIF de la compañía.
     * 
     * @return CIF de la compañía
     */
    public String getCif() {
        return cif;
    }
    
    /**
     * Establece el CIF de la compañía.
     * 
     * @param cif CIF de la compañía (debe seguir formato: 8 números + 1 letra)
     */
    public void setCif(String cif) {
        this.cif = cif;
    }
    
    /**
     * Obtiene el nombre de la compañía.
     * 
     * @return Nombre de la compañía
     */
    public String getNombre() {
        return nombre;
    }
    
    /**
     * Establece el nombre de la compañía.
     * 
     * @param nombre Nombre de la compañía
     */
    public void setNombre(String nombre) {
        this.nombre = nombre;
    }
    
    /**
     * Obtiene el sector de la compañía.
     * 
     * @return Sector de la compañía
     */
    public String getSector() {
        return sector;
    }
    
    /**
     * Establece el sector de la compañía.
     * 
     * @param sector Sector de la compañía
     */
    public void setSector(String sector) {
        this.sector = sector;
    }
    
    // === MÉTODOS UTILITARIOS ===
    
    /**
     * Valida si el CIF tiene un formato correcto (8 números + 1 letra).
     * 
     * @return true si el CIF es válido, false en caso contrario
     */
    public boolean isValidCif() {
        if (cif == null || cif.length() != 9) {
            return false;
        }
        
        // Verificar que los primeros 8 caracteres sean números
        String numeros = cif.substring(0, 8);
        if (!numeros.matches("\\d{8}")) {
            return false;
        }
        
        // Verificar que el último carácter sea una letra
        char letra = cif.charAt(8);
        return Character.isLetter(letra);
    }
    
    /**
     * Verifica si todos los campos obligatorios están completados.
     * 
     * @return true si todos los campos están completos, false en caso contrario
     */
    public boolean isComplete() {
        return cif != null && !cif.trim().isEmpty() &&
               nombre != null && !nombre.trim().isEmpty() &&
               sector != null && !sector.trim().isEmpty();
    }
    
    /**
     * Representación en cadena de la compañía.
     * 
     * @return Representación textual de la compañía
     */
    @Override
    public String toString() {
        return String.format("Company{CIF='%s', Nombre='%s', Sector='%s'}", 
                           cif, nombre, sector);
    }
    
    /**
     * Compara dos objetos Company por igualdad.
     * Dos compañías son iguales si tienen el mismo CIF.
     * 
     * @param obj Objeto a comparar
     * @return true si son iguales, false en caso contrario
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        Company company = (Company) obj;
        return cif != null ? cif.equals(company.cif) : company.cif == null;
    }
    
    /**
     * Calcula el hash code basado en el CIF.
     * 
     * @return Hash code del objeto
     */
    @Override
    public int hashCode() {
        return cif != null ? cif.hashCode() : 0;
    }
}