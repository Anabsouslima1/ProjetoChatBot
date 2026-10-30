# Sistema de troca de mensagens instantâneas

Projeto da disciplina de Sistemas Distribuídos. Bots (clientes) se conectam a servidores
através de um broker usando ZeroMQ. Esta primeira parte implementa login, criação de
canais e listagem de canais, com persistência em disco.

- Linguagens: C++, Java e Node.js
- Mensagens: ZeroMQ (REQ/REP com broker ROUTER/DEALER)
- Serialização: Protocol Buffers (binário) — contrato em `proto/chat.proto` 
- Persistência: Arquivos de texto por servidor (`logins.tsv` e `channels.txt`) em um volume próprio
- Containers: Docker + Docker Compose 

### Mensagens
Toda mensagem trafega em um Envelope que contém `timestamp_ms`, `sender` e um
`oneof body` com uma das mensagens: `LoginRequest/Response`, `CreateChannelRequest/Response`,
`ListChannelsRequest/Response`.

### Regras e erros
- Usuário e canal: 1–32 caracteres entre `A-Z a-z 0-9 _ -`.
- Erros (`ErrorCode`): usuário inválido, canal inválido, canal duplicado, requisição desconhecida, erro interno.
- Login com erro é repetido pelo bot a cada 2 s. Criação de canal duplicado retorna erro; o bot só cria canais que não aparecem na listagem.

### Persistência
Cada servidor grava em seu próprio volume (`/data`): `logins.tsv` (`timestamp_ms<TAB>usuário`)
e `channels.txt` (um canal por linha), carregado ao iniciar.

### Limitação conhecida (parte 1)
O broker distribui as requisições entre os servidores em round-robin e, como cada servidor
mantém seu próprio conjunto de dados, os canais criados em um servidor não aparecem nos outros. Assim, a listagem de canais pode variar conforme o servidor que responde, e um bot pode receber `ERROR_CHANNEL_EXISTS` ao tentar criar um canal que não apareceu na sua listagem. O erro é tratado normalmente.

## Estrutura
```
proto/chat.proto        contrato de mensagens (compartilhado)
cpp/src/broker.cpp      broker (ROUTER 5555 <-> DEALER 5556)
cpp/src/proxy.cpp       proxy Pub/Sub (XSUB 5557 <-> XPUB 5558)
cpp/src/server.cpp      servidor (REP + PUB)
cpp/src/client.cpp      bot cliente (REQ + SUB)
Dockerfile              imagem C++
docker-compose.yaml     2 servidores + 2 clientes C++ + broker + proxy
docker-compose.host.yaml override opcional (rede do host), ver abaixo
```

## Como executar
```bash
docker compose up --build
```
Cada envio/recebimento é exibido no formato `timestamp | quem | ENVIADO/RECEBIDO | de=... | conteúdo`.

### Ambientes com rede bridge bloqueada (ex.: alguns GitHub Codespaces)
Em alguns ambientes o tráfego entre containers da rede bridge do Docker é bloqueado, e os bots ficam sem resposta no login. Nesse caso, use o arquivo de override, que executa os containers na rede do host (os endereços passam a ser `127.0.0.1`):
```bash
docker compose -f docker-compose.yaml -f docker-compose.host.yaml up --build
```
A execução padrão (`docker compose up`) continua usando a rede criada pelo Docker Compose.

### Fluxo
```
Bot  --REQ publish_request(canal, mensagem)-->  Servidor   (via broker, 5555/5556)
Servidor valida, grava em disco e publica:
Servidor --PUB [tópico=canal][Envelope(publication)]-->  Proxy (XSUB 5557)
Proxy --XPUB 5558--> Bots inscritos naquele canal (SUB)
Servidor --REP publish_response(ok/erro)-->  Bot
```
O `proxy` é um container separado do broker (`cpp/src/proxy.cpp`): servidores conectam o `PUB` no `XSUB` (5557) e os bots conectam o `SUB` no `XPUB` (5558). Endereços por variável de ambiente: `PROXY_XSUB` (servidores) e `PROXY_XPUB` (bots).

### Mensagens (`proto/chat.proto`)
- `PublishRequest{channel, message}` / `PublishResponse{success, error, error_message}` (Req/Rep).
- `Publication{channel, message, author}` publicada no Pub/Sub, dentro de um `Envelope`.
  Cada publicação tem **2 frames**: `[nome do canal (tópico)]` e `[Envelope serializado]`.
- O `timestamp_ms` do `Envelope` é o horário de envio (do pedido e da publicação). O bot
  mede o horário de recebimento no próprio relógio e exibe os dois.
- Novos erros: `ERROR_CHANNEL_NOT_FOUND` (reservado) e `ERROR_INVALID_MESSAGE` (vazia ou
  com mais de 1000 bytes).

### Inscrição em canais
A inscrição é feita só no cliente: um único `SUB` pode assinar vários tópicos. Como a inscrição do ZeroMQ casa por **prefixo** (`geral` também receberia `geral2`), o bot descarta o que não for exatamente de um canal em que se inscreveu. A tela mostra o canal, o autor, a mensagem, o horário de envio e o de recebimento. O `SUB` roda em uma thread própria (um socket ZeroMQ não pode ser usado por duas threads).

### Rotina padronizada dos bots
Depois do login, o bot repete indefinidamente, a cada rodada:
1. lista os canais; se houver menos de 5, cria um novo;
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um, escolhido ao acaso;
3. escolhe um canal da lista e publica 10 mensagens aleatórias, 1 por segundo.

Interpretamos os passos 1 e 2 como verificados a cada rodada (e não só uma vez ao conectar),
para que o bot chegue a 5 canais e 3 inscrições mesmo que a lista varie entre servidores.

### Persistência das publicações
Cada servidor grava em `/data/publications.bin` (arquivo próprio, não compartilhado) um
registro por publicação: 4 bytes (big-endian) com o tamanho, seguidos do `Envelope`
protobuf com canal, autor, texto e timestamp. É um formato binário que preserva todas as
informações e permite recuperar tudo, e o próprio servidor lê o arquivo:
```bash
docker compose exec server_cpp_1 /app/server --dump /data/publications.bin
```

### Limitações conhecidas
- Cada servidor conhece só os canais criados nele. Por isso o servidor aceita publicar em
  qualquer nome de canal **válido** sem checar se ele existe ali; caso contrário as publicações falhariam conforme o servidor sorteado pelo broker. A consistência entre servidores (replicação) fica como evolução.
- Pub/Sub não guarda histórico: um bot só recebe o que for publicado depois da inscrição (as mensagens ficam gravadas em disco nos servidores).
