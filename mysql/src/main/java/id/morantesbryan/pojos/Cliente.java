package id.morantesbryan.pojos;

// Clase auxiliar para representar los datos de un cliente (POJO)
public class Cliente {
    private String dni;
    private String apellidos;
    private int codigoPostal;

    public Cliente(String dni, String apellidos, int codigoPostal) {
        this.dni = dni;
        this.apellidos = apellidos;
        this.codigoPostal = codigoPostal;
    }

    // Getters
    public String getDni() { return dni; }
    public String getApellidos() { return apellidos; }
    public int getCodigoPostal() { return codigoPostal; }

    @Override
    public String toString() {
        return "Cliente{" +
                "dni='" + dni + '\'' +
                ", apellidos='" + apellidos + '\'' +
                ", codigoPostal=" + codigoPostal +
                '}';
    }
}
