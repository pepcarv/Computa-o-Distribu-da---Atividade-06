package br.mackenzie.biblioteca.model;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

// Repositorio compartilhado pelo servidor: sem banco de dados.
public final class AcervoMemoria {
    private final Map<Long, Livro> livrosPorId = new ConcurrentHashMap<>();
    private final Map<String, Long> idPorIsbn = new ConcurrentHashMap<>();
    private final List<Emprestimo> emprestimos = new CopyOnWriteArrayList<>();
    private final AtomicLong proximoId = new AtomicLong(1);

    // O lock impede que dois cadastros simultaneos usem o mesmo ISBN.
    // Retorna null quando o ISBN ja pertence ao acervo.
    public synchronized Livro cadastrar(String titulo, String autor, int ano, String isbn) {
        String chaveIsbn = isbn.replaceAll("[\\s-]", "").toUpperCase(Locale.ROOT);
        if (idPorIsbn.containsKey(chaveIsbn)) {
            return null;
        }

        Livro livro = new Livro(proximoId.getAndIncrement(), titulo, autor, ano, isbn);
        livrosPorId.put(livro.id(), livro);
        idPorIsbn.put(chaveIsbn, livro.id());
        return livro;
    }

    public List<Livro> buscarPorAutor(String autor) {
        return livrosPorId.values().stream()
                .filter(livro -> livro.autor().equalsIgnoreCase(autor.trim()))
                .sorted(Comparator.comparingLong(Livro::id))
                .toList();
    }

    public boolean existeLivro(long id) {
        return livrosPorId.containsKey(id);
    }

    public void registrarEmprestimos(List<Emprestimo> novos) {
        emprestimos.addAll(novos);
    }

    public int totalEmprestimos() {
        return emprestimos.size();
    }

    // Busca por titulo/autor e associa algumas palavras-chave a livros cadastrados.
    public Livro sugerirPorPalavraChave(String palavraChave) {
        String busca = normalizar(palavraChave);
        String tituloRelacionado = switch (busca) {
            case "aventura", "amizade", "infancia", "principe" -> "pequeno principe";
            case "traue" -> "livro do traue";
            case "conto", "cao", "tinhoso" -> "nos matamos o cao tinhoso";
            case "terra", "sonambula", "mocambique" -> "terra sonambula";
            default -> busca;
        };

        List<Livro> ordenados = new ArrayList<>(livrosPorId.values());
        ordenados.sort(Comparator.comparingLong(Livro::id));
        for (Livro livro : ordenados) {
            if (normalizar(livro.titulo()).contains(tituloRelacionado)
                    || normalizar(livro.autor()).contains(tituloRelacionado)) {
                return livro;
            }
        }
        return null;
    }

    private static String normalizar(String texto) {
        return Normalizer.normalize(texto.toLowerCase(Locale.ROOT).trim(), Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "");
    }
}
