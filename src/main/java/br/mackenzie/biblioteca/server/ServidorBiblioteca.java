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
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

public final class ServidorBiblioteca {
    private static final int PORTA = 50051;

    public static void main(String[] args) throws IOException, InterruptedException {
        AcervoMemoria acervo = new AcervoMemoria();
        Server servidor = ServerBuilder.forPort(PORTA)
                .addService(new BibliotecaServiceImpl(acervo))
                .build()
                .start();

        System.out.printf("Servidor Biblioteca gRPC iniciado na porta %d.%n", PORTA);
        System.out.println("Aguardando chamadas RPC... (Ctrl+C para encerrar)");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Encerrando servidor Biblioteca gRPC...");
            servidor.shutdown();
        }));

        servidor.awaitTermination();
    }
}
