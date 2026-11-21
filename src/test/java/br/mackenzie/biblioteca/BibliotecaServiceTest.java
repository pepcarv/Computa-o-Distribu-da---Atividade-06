package br.mackenzie.biblioteca;

import br.mackenzie.biblioteca.model.AcervoMemoria;
import br.mackenzie.biblioteca.proto.BibliotecaServiceGrpc;
import br.mackenzie.biblioteca.proto.CadastrarLivroRequest;
import br.mackenzie.biblioteca.proto.ListarLivrosPorAutorRequest;
import br.mackenzie.biblioteca.proto.LivroResponse;
import br.mackenzie.biblioteca.proto.MensagemChatRequest;
import br.mackenzie.biblioteca.proto.RegistrarEmprestimoRequest;
import br.mackenzie.biblioteca.proto.ResumoEmprestimosResponse;
import br.mackenzie.biblioteca.proto.SugestaoLivroResponse;
import br.mackenzie.biblioteca.server.BibliotecaServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BibliotecaServiceTest {
    private Server servidor;
    private ManagedChannel canal;
    private BibliotecaServiceGrpc.BibliotecaServiceBlockingStub blocking;
    private BibliotecaServiceGrpc.BibliotecaServiceStub async;
    private AcervoMemoria acervo;

    @BeforeEach
    void preparar() throws Exception {
        String nome = "biblioteca-teste-" + UUID.randomUUID();
        acervo = new AcervoMemoria();
        servidor = InProcessServerBuilder.forName(nome).directExecutor()
                .addService(new BibliotecaServiceImpl(acervo)).build().start();
        canal = InProcessChannelBuilder.forName(nome).directExecutor().build();
        blocking = BibliotecaServiceGrpc.newBlockingStub(canal);
        async = BibliotecaServiceGrpc.newStub(canal);
    }

    @AfterEach
    void encerrar() throws Exception {
        canal.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        servidor.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    private long cadastrar(String titulo, String autor, String isbn) {
        return blocking.cadastrarLivro(CadastrarLivroRequest.newBuilder()
                .setTitulo(titulo).setAutor(autor).setAno(2000).setIsbn(isbn).build()).getId();
    }

    @Test
    void cadastroUnaryGeraIdsDiferentes() {
        long primeiro = cadastrar("Livro A", "Autor X", "111");
        long segundo = cadastrar("Livro B", "Autor Y", "222");
        assertEquals(1, primeiro);
        assertEquals(2, segundo);
    }

    @Test
    void cadastroRejeitaIsbnDuplicado() {
        cadastrar("Livro A", "Autor X", "978-11");
        StatusRuntimeException erro = assertThrows(StatusRuntimeException.class,
                () -> cadastrar("Livro B", "Autor Y", "97811"));
        assertEquals(Status.Code.ALREADY_EXISTS, erro.getStatus().getCode());
    }

    @Test
    void cadastroRejeitaTituloVazio() {
        StatusRuntimeException erro = assertThrows(StatusRuntimeException.class,
                () -> cadastrar(" ", "Autor X", "123"));
        assertEquals(Status.Code.INVALID_ARGUMENT, erro.getStatus().getCode());
    }

    @Test
    void serverStreamingEnviaLivrosDoAutorUmPorUm() {
        cadastrar("Livro 1", "traue", "111");
        cadastrar("Livro 2", "TRAUE", "222");
        cadastrar("Outro", "Mia Couto", "333");
        Iterator<LivroResponse> stream = blocking.listarLivrosPorAutor(
                ListarLivrosPorAutorRequest.newBuilder().setAutor("traue").build());
        List<LivroResponse> livros = new ArrayList<>();
        stream.forEachRemaining(livros::add);
        assertEquals(2, livros.size());
        assertEquals("Livro 1", livros.get(0).getTitulo());
        assertEquals("Livro 2", livros.get(1).getTitulo());
    }

    @Test
    void autorInexistenteRetornaStreamVazia() {
        Iterator<LivroResponse> stream = blocking.listarLivrosPorAutor(
                ListarLivrosPorAutorRequest.newBuilder().setAutor("Nao Existe").build());
        assertFalse(stream.hasNext());
    }

    @Test
    void clientStreamingRegistraCincoEmprestimos() throws Exception {
        long livroId = cadastrar("Livro", "Autor", "100");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ResumoEmprestimosResponse> resumo = new AtomicReference<>();
        AtomicReference<Throwable> erro = new AtomicReference<>();
        StreamObserver<RegistrarEmprestimoRequest> pedido = async.registrarEmprestimos(
                new StreamObserver<>() {
                    public void onNext(ResumoEmprestimosResponse r) { resumo.set(r); }
                    public void onError(Throwable t) { erro.set(t); latch.countDown(); }
                    public void onCompleted() { latch.countDown(); }
                });
        for (int i = 0; i < 5; i++) {
            pedido.onNext(RegistrarEmprestimoRequest.newBuilder()
                    .setUsuario("Usuario" + i).setLivroId(livroId).build());
        }
        pedido.onCompleted();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(erro.get());
        assertNotNull(resumo.get());
        assertEquals(5, resumo.get().getTotalEmprestimos());
        assertTrue(resumo.get().getTempoProcessamentoMs() >= 0);
        assertEquals(5, acervo.totalEmprestimos());
    }

    @Test
    void clientStreamingRejeitaLivroInexistente() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> erro = new AtomicReference<>();
        StreamObserver<RegistrarEmprestimoRequest> pedido = async.registrarEmprestimos(
                new StreamObserver<>() {
                    public void onNext(ResumoEmprestimosResponse r) { fail("Nao deveria responder"); }
                    public void onError(Throwable t) { erro.set(t); latch.countDown(); }
                    public void onCompleted() { latch.countDown(); }
                });
        pedido.onNext(RegistrarEmprestimoRequest.newBuilder()
                .setUsuario("Usuario").setLivroId(999L).build());
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(Status.Code.NOT_FOUND, Status.fromThrowable(erro.get()).getCode());
        assertEquals(0, acervo.totalEmprestimos());
    }

    @Test
    void chatBidirecionalRespondeTresMensagens() throws Exception {
        cadastrar("Pequeno Principe", "Antoine", "111");
        cadastrar("Livro do Traue", "traue", "222");
        cadastrar("Terra Sonambula", "Mia Couto", "333");
        CountDownLatch latch = new CountDownLatch(1);
        List<SugestaoLivroResponse> respostas = new ArrayList<>();
        AtomicReference<Throwable> erro = new AtomicReference<>();
        StreamObserver<MensagemChatRequest> pedido = async.chatBibliotecario(
                new StreamObserver<>() {
                    public void onNext(SugestaoLivroResponse r) { respostas.add(r); }
                    public void onError(Throwable t) { erro.set(t); latch.countDown(); }
                    public void onCompleted() { latch.countDown(); }
                });
        for (String palavra : List.of("aventura", "traue", "terra")) {
            pedido.onNext(MensagemChatRequest.newBuilder().setPalavraChave(palavra).build());
        }
        pedido.onCompleted();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(erro.get());
        assertEquals(3, respostas.size());
        assertEquals("Pequeno Principe", respostas.get(0).getTituloSugerido());
        assertEquals("Livro do Traue", respostas.get(1).getTituloSugerido());
        assertEquals("Terra Sonambula", respostas.get(2).getTituloSugerido());
    }
}
