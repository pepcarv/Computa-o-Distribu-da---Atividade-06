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

package br.mackenzie.biblioteca.server;

import br.mackenzie.biblioteca.model.AcervoMemoria;
import br.mackenzie.biblioteca.model.Emprestimo;
import br.mackenzie.biblioteca.model.Livro;
import br.mackenzie.biblioteca.proto.BibliotecaServiceGrpc;
import br.mackenzie.biblioteca.proto.CadastrarLivroRequest;
import br.mackenzie.biblioteca.proto.CadastrarLivroResponse;
import br.mackenzie.biblioteca.proto.ListarLivrosPorAutorRequest;
import br.mackenzie.biblioteca.proto.LivroResponse;
import br.mackenzie.biblioteca.proto.MensagemChatRequest;
import br.mackenzie.biblioteca.proto.RegistrarEmprestimoRequest;
import br.mackenzie.biblioteca.proto.ResumoEmprestimosResponse;
import br.mackenzie.biblioteca.proto.SugestaoLivroResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;

public final class BibliotecaServiceImpl extends BibliotecaServiceGrpc.BibliotecaServiceImplBase {
    private final AcervoMemoria acervo;

    public BibliotecaServiceImpl(AcervoMemoria acervo) {
        this.acervo = acervo;
    }

    // 1. Unary RPC
    @Override
    public void cadastrarLivro(CadastrarLivroRequest request,
                               StreamObserver<CadastrarLivroResponse> responseObserver) {
        System.out.printf("[RPC cadastrarLivro] titulo='%s', autor='%s', ano=%d, ISBN='%s'%n",
                request.getTitulo(), request.getAutor(), request.getAno(), request.getIsbn());

        String titulo = request.getTitulo().trim();
        String autor = request.getAutor().trim();
        String isbn = request.getIsbn().trim();
        if (titulo.isEmpty() || autor.isEmpty() || isbn.isEmpty() || request.getAno() <= 0
                || isbn.replaceAll("[\\s-]", "").isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Titulo, autor, ano positivo e ISBN valido sao obrigatorios.")
                    .asRuntimeException());
            return;
        }

        Livro livro = acervo.cadastrar(titulo, autor, request.getAno(), isbn);
        if (livro == null) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription("Ja existe livro cadastrado com o ISBN informado: " + isbn)
                    .asRuntimeException());
            return;
        }

        responseObserver.onNext(CadastrarLivroResponse.newBuilder()
                .setId(livro.id())
                .setStatus("CADASTRADO")
                .setMensagem("Livro cadastrado com sucesso.")
                .build());
        responseObserver.onCompleted();
    }

    // 2. Server Streaming RPC
    @Override
    public void listarLivrosPorAutor(ListarLivrosPorAutorRequest request,
                                     StreamObserver<LivroResponse> responseObserver) {
        System.out.printf("[RPC listarLivrosPorAutor] autor='%s'%n", request.getAutor());
        String autor = request.getAutor().trim();
        if (autor.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Informe o autor para realizar a busca.")
                    .asRuntimeException());
            return;
        }

        // Autor inexistente: retorna stream vazia, como permitido no enunciado.
        for (Livro livro : acervo.buscarPorAutor(autor)) {
            responseObserver.onNext(LivroResponse.newBuilder()
                    .setId(livro.id())
                    .setTitulo(livro.titulo())
                    .setAutor(livro.autor())
                    .setAno(livro.ano())
                    .setIsbn(livro.isbn())
                    .build());
        }
        responseObserver.onCompleted();
    }

    // 3. Client Streaming RPC
    @Override
    public StreamObserver<RegistrarEmprestimoRequest> registrarEmprestimos(
            StreamObserver<ResumoEmprestimosResponse> responseObserver) {
        System.out.println("[RPC registrarEmprestimos] stream iniciada");
        return new StreamObserver<>() {
            private final long inicioNanos = System.nanoTime();
            private final List<Emprestimo> recebidos = new ArrayList<>();
            private boolean encerrado;

            @Override
            public void onNext(RegistrarEmprestimoRequest request) {
                if (encerrado) {
                    return;
                }
                System.out.printf("[RPC registrarEmprestimos] usuario='%s', livro_id=%d%n",
                        request.getUsuario(), request.getLivroId());

                if (request.getUsuario().trim().isEmpty() || request.getLivroId() <= 0) {
                    encerrarComErro(Status.INVALID_ARGUMENT
                            .withDescription("Usuario e livro_id positivo sao obrigatorios."));
                    return;
                }
                if (!acervo.existeLivro(request.getLivroId())) {
                    encerrarComErro(Status.NOT_FOUND
                            .withDescription("Livro nao encontrado: " + request.getLivroId()));
                    return;
                }
                recebidos.add(new Emprestimo(request.getUsuario().trim(), request.getLivroId()));
            }

            @Override
            public void onError(Throwable erro) {
                encerrado = true;
                System.out.println("[RPC registrarEmprestimos] stream cancelada: " + erro.getMessage());
            }

            @Override
            public void onCompleted() {
                if (encerrado) {
                    return;
                }
                encerrado = true;
                acervo.registrarEmprestimos(recebidos);
                double tempoMs = (System.nanoTime() - inicioNanos) / 1_000_000.0;
                System.out.printf("[RPC registrarEmprestimos] total=%d, tempo=%.3f ms%n",
                        recebidos.size(), tempoMs);
                responseObserver.onNext(ResumoEmprestimosResponse.newBuilder()
                        .setTotalEmprestimos(recebidos.size())
                        .setTempoProcessamentoMs(tempoMs)
                        .build());
                responseObserver.onCompleted();
            }

            private void encerrarComErro(Status status) {
                encerrado = true;
                responseObserver.onError(status.asRuntimeException());
            }
        };
    }

    // 4. Bidirectional Streaming RPC
    @Override
    public StreamObserver<MensagemChatRequest> chatBibliotecario(
            StreamObserver<SugestaoLivroResponse> responseObserver) {
        System.out.println("[RPC chatBibliotecario] stream iniciada");
        return new StreamObserver<>() {
            private boolean encerrado;

            @Override
            public void onNext(MensagemChatRequest request) {
                if (encerrado) {
                    return;
                }
                System.out.printf("[RPC chatBibliotecario] palavra_chave='%s'%n",
                        request.getPalavraChave());
                String chave = request.getPalavraChave().trim();
                if (chave.isEmpty()) {
                    encerrado = true;
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("A palavra-chave nao pode ser vazia.")
                            .asRuntimeException());
                    return;
                }

                Livro sugestao = acervo.sugerirPorPalavraChave(chave);
                SugestaoLivroResponse.Builder resposta = SugestaoLivroResponse.newBuilder()
                        .setPalavraChave(chave);
                if (sugestao == null) {
                    resposta.setMensagem("Nenhum livro relacionado a palavra-chave foi encontrado.");
                } else {
                    resposta.setLivroId(sugestao.id())
                            .setTituloSugerido(sugestao.titulo())
                            .setAutorSugerido(sugestao.autor())
                            .setMensagem("Sugestao encontrada no acervo.");
                }
                responseObserver.onNext(resposta.build());
            }

            @Override
            public void onError(Throwable erro) {
                encerrado = true;
                System.out.println("[RPC chatBibliotecario] stream cancelada: " + erro.getMessage());
            }

            @Override
            public void onCompleted() {
                if (!encerrado) {
                    encerrado = true;
                    responseObserver.onCompleted();
                }
            }
        };
    }
}
