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
cpp/src/server.cpp      servidor (REP)
cpp/src/client.cpp      bot cliente (REQ)
Dockerfile              imagem C++
docker-compose.yaml     2 servidores + 2 clientes C++ + broker
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
