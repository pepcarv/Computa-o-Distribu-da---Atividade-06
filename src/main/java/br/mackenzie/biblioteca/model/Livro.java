package br.mackenzie.biblioteca.model;

// Entidade em memoria; os dados compartilhados entre cliente e servidor estao no .proto.
public record Livro(long id, String titulo, String autor, int ano, String isbn) {
}
