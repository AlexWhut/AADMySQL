package id.morantesbryan.pojos;

public class ResultadoListado {
    private final int contadorInOut; // Valor del parámetro INOUT
    private final java.util.List<Cliente> clientes; // Lista de clientes recuperada

    public ResultadoListado(int contadorInOut, java.util.List<Cliente> clientes) {
        this.contadorInOut = contadorInOut;
        this.clientes = clientes;
    }

    public int getContadorInOut() { return contadorInOut; }
    public java.util.List<Cliente> getClientes() { return clientes; }
}
