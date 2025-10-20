package id.morantesbryan.pojos;

// Clase auxiliar para representar una línea de factura (POJO)
public class LineaFactura {
    private String concepto;
    private int cantidad;

    public LineaFactura(String concepto, int cantidad) {
        this.concepto = concepto;
        this.cantidad = cantidad;
    }

    public String getConcepto() {
        return concepto;
    }

    public int getCantidad() {
        return cantidad;
    }
}
