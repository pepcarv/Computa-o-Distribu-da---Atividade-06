# Atividade: Sistema de Gerenciamento de Biblioteca Digital com gRPC

Aplicação em Java 17 e Maven que demonstra os quatro tipos de RPC (Unary, Server Streaming, Client Streaming e Bidirectional Streaming) usando Protocol Buffers, com livros e empréstimos armazenados em memória.

## Integrantes
- Pedro Henrique Carvalho Pereira - 10418861
- Mateus Ribeiro Cerqueira - 10443901


## Compilação

Dentro da pasta `biblioteca-grpc`:

```bash
mvn clean compile
mvn clean package
```


## Execução

Abrir dois terminais na pasta do projeto. Executar primeiro o servidor e manter esse terminal aberto.

**Terminal 1 - Servidor:**

```bash
mvn exec:java -Dexec.mainClass=br.mackenzie.biblioteca.server.ServidorBiblioteca
```

**Terminal 2 - Cliente:**

```bash
mvn exec:java -Dexec.mainClass=br.mackenzie.biblioteca.client.ClienteBiblioteca
```


## Resultado

FALTA ADICIONAR AS PRINTS 
