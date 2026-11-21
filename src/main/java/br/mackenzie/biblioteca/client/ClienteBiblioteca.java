/*
Alunos:
- Pedro Henrique Carvalho Pereira - 10418861
- Mateus Ribeiro Cerqueira - 10443901

Compilação e execução:
mvn clean compile
mvn -q compile exec:java -Dexec.mainClass=br.mackenzie.biblioteca.server.ServidorBiblioteca
mvn -q compile exec:java -Dexec.mainClass=br.mackenzie.biblioteca.client.ClienteBiblioteca

Referências extra aula:
- https://grpc.io/docs/languages/java/
- https://grpc.io/docs/what-is-grpc/core-concepts/
- https://protobuf.dev/programming-guides/proto3/

OBS: os dados ficam em memória e os quatro tipos de RPC são definidos no biblioteca.proto.
*/

package br.mackenzie.biblioteca.client;

import br.mackenzie.biblioteca.proto.BibliotecaServiceGrpc;
import br.mackenzie.biblioteca.proto.CadastrarLivroRequest;
import br.mackenzie.biblioteca.proto.CadastrarLivroResponse;
import br.mackenzie.biblioteca.proto.ListarLivrosPorAutorRequest;
import br.mackenzie.biblioteca.proto.LivroResponse;
import br.mackenzie.biblioteca.proto.MensagemChatRequest;
import br.mackenzie.biblioteca.proto.RegistrarEmprestimoRequest;
import br.mackenzie.biblioteca.proto.ResumoEmprestimosResponse;
import br.mackenzie.biblioteca.proto.SugestaoLivroResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class ClienteBiblioteca {
    private final BibliotecaServiceGrpc.BibliotecaServiceBlockingStub stubBloqueante;
    private final BibliotecaServiceGrpc.BibliotecaServiceStub stubAssincrono;

    private ClienteBiblioteca(ManagedChannel canal) {
        this.stubBloqueante = BibliotecaServiceGrpc.newBlockingStub(canal);
        this.stubAssincrono = BibliotecaServiceGrpc.newStub(canal);
    }

    public static void main(String[] args) throws InterruptedException {
        ManagedChannel canal = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext() // Apenas para testes locais, sem TLS.
                .build();
        try {
            ClienteBiblioteca cliente = new ClienteBiblioteca(canal);
            cliente.executarDemonstracao();
        } finally {
            canal.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private void executarDemonstracao() throws InterruptedException {
        System.out.println("========== DEMONSTRACAO: BIBLIOTECA gRPC ==========");
        System.out.println("Inicie o servidor vazio antes de executar a demonstracao.\n");

        // 1) Cadastra os QUATRO livros solicitados (o minimo era tres).
        System.out.println("[1] Unary RPC - cadastrarLivro");
        List<Long> ids = new ArrayList<>();
        ids.add(cadastrar("Pequeno Principe", "Antoine de Saint-Exupéry", 1943, "9780156012195"));
        ids.add(cadastrar("Livro do Traue", "traue", 2024, "9786500000002"));
        ids.add(cadastrar("Nós Matamos o Cão Tinhoso", "Luís Bernardo Honwana", 1964, "9786500000003"));
        ids.add(cadastrar("Terra Sonambula", "Mia Couto", 1992, "9786500000004"));

        // 2) Autor existente e autor inexistente.
        System.out.println("\n[2] Server Streaming RPC - listarLivrosPorAutor");
        listarPorAutor("traue");
        listarPorAutor("Autor Inexistente");

        // 3) Cinco emprestimos consecutivos em UM stream.
        System.out.println("\n[3] Client Streaming RPC - registrarEmprestimos");
        if (ids.stream().allMatch(id -> id != null && id > 0)) {
            registrarCincoEmprestimos(ids);
        } else {
            System.out.println("Nao foi possivel registrar emprestimos: reinicie o servidor para limpar os cadastros.");
        }

        // 4) Tres mensagens em uma unica conexao bidirecional.
        System.out.println("\n[4] Bidirectional Streaming RPC - chatBibliotecario");
        conversar("aventura", "traue", "moçambique");

        // Teste de erro exigido no enunciado.
        System.out.println("\n[5] Teste de erro - ISBN duplicado");
        cadastrar("Pequeno Principe (duplicado)", "Outro Autor", 2026, "9780156012195");

        System.out.println("\n========== FIM DA DEMONSTRACAO ==========");
    }

    private Long cadastrar(String titulo, String autor, int ano, String isbn) {
        CadastrarLivroRequest request = CadastrarLivroRequest.newBuilder()
                .setTitulo(titulo)
                .setAutor(autor)
                .setAno(ano)
                .setIsbn(isbn)
                .build();
        try {
            CadastrarLivroResponse resposta = stubBloqueante.cadastrarLivro(request);
            System.out.printf("  OK | id=%d | status=%s | titulo=%s | ISBN=%s%n",
                    resposta.getId(), resposta.getStatus(), titulo, isbn);
            return resposta.getId();
        } catch (StatusRuntimeException erro) {
            exibirErro(erro);
            return null;
        }
    }

    private void listarPorAutor(String autor) {
        System.out.printf("  Buscando autor: %s%n", autor);
        int total = 0;
        try {
            Iterator<LivroResponse> livros = stubBloqueante.listarLivrosPorAutor(
                    ListarLivrosPorAutorRequest.newBuilder().setAutor(autor).build());
            while (livros.hasNext()) {
                LivroResponse livro = livros.next();
                total++;
                System.out.printf("    Livro recebido: id=%d | %s | %s | %d%n",
                        livro.getId(), livro.getTitulo(), livro.getAutor(), livro.getAno());
            }
            if (total == 0) {
                System.out.println("    Stream vazia: nenhum livro encontrado.");
            }
        } catch (StatusRuntimeException erro) {
            exibirErro(erro);
        }
    }

    private void registrarCincoEmprestimos(List<Long> ids) throws InterruptedException {
        CountDownLatch finalizado = new CountDownLatch(1);
        AtomicReference<Throwable> erroRecebido = new AtomicReference<>();
        StreamObserver<ResumoEmprestimosResponse> respostas = new StreamObserver<>() {
            @Override
            public void onNext(ResumoEmprestimosResponse resumo) {
                System.out.printf("  RESUMO | total=%d | processamento=%.3f ms%n",
                        resumo.getTotalEmprestimos(), resumo.getTempoProcessamentoMs());
            }

            @Override
            public void onError(Throwable erro) {
                erroRecebido.set(erro);
                finalizado.countDown();
            }

            @Override
            public void onCompleted() {
                finalizado.countDown();
            }
        };

        StreamObserver<RegistrarEmprestimoRequest> requisicoes =
                stubAssincrono.registrarEmprestimos(respostas);
        for (int i = 0; i < 5; i++) {
            long id = ids.get(i % ids.size());
            String usuario = "usuario" + (i + 1);
            System.out.printf("  Enviando: usuario=%s, livro_id=%d%n", usuario, id);
            requisicoes.onNext(RegistrarEmprestimoRequest.newBuilder()
                    .setUsuario(usuario)
                    .setLivroId(id)
                    .build());
        }
        requisicoes.onCompleted();
        aguardar(finalizado, "registrarEmprestimos");
        if (erroRecebido.get() != null) {
            exibirErro(erroRecebido.get());
        }
    }

    private void conversar(String... palavrasChave) throws InterruptedException {
        CountDownLatch finalizado = new CountDownLatch(1);
        AtomicReference<Throwable> erroRecebido = new AtomicReference<>();
        StreamObserver<SugestaoLivroResponse> respostas = new StreamObserver<>() {
            @Override
            public void onNext(SugestaoLivroResponse sugestao) {
                if (sugestao.getLivroId() == 0) {
                    System.out.printf("  Bibliotecario [%s]: %s%n",
                            sugestao.getPalavraChave(), sugestao.getMensagem());
                } else {
                    System.out.printf("  Bibliotecario [%s]: %s (autor: %s)%n",
                            sugestao.getPalavraChave(), sugestao.getTituloSugerido(),
                            sugestao.getAutorSugerido());
                }
            }

            @Override
            public void onError(Throwable erro) {
                erroRecebido.set(erro);
                finalizado.countDown();
            }

            @Override
            public void onCompleted() {
                finalizado.countDown();
            }
        };

        StreamObserver<MensagemChatRequest> requisicoes =
                stubAssincrono.chatBibliotecario(respostas);
        for (String chave : palavrasChave) {
            System.out.println("  Usuario: " + chave);
            requisicoes.onNext(MensagemChatRequest.newBuilder().setPalavraChave(chave).build());
        }
        requisicoes.onCompleted();
        aguardar(finalizado, "chatBibliotecario");
        if (erroRecebido.get() != null) {
            exibirErro(erroRecebido.get());
        }
    }

    private static void aguardar(CountDownLatch latch, String metodo) throws InterruptedException {
        if (!latch.await(15, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Tempo limite excedido para o RPC " + metodo);
        }
    }

    private static void exibirErro(Throwable erro) {
        Status status = Status.fromThrowable(erro);
        System.out.printf("  ERRO %s | %s%n", status.getCode(), status.getDescription());
    }
}
