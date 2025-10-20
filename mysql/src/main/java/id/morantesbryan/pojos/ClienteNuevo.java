package id.morantesbryan.pojos;

/**
 * Un 'record' para encapsular los datos de un nuevo cliente.
 * Es una forma moderna y concisa de crear una clase de datos inmutable.
 */
public record ClienteNuevo(String dni, String apellidos, String cp) {}
