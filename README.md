# Projeto PolyGlot – Um Chatbot Distribuído

PolyGlot é um sistema distribuído de troca de mensagens instântaneas, desenvolvido em C++, Java e Node.js. Utiliza ZeroMQ para troca de mensagens, Protocol Buffers (binário) para serialização e Docker para conteinerização.

O projeto explora a comunicação entre aplicações implementadas em diferentes linguagens, por meio de uma arquitetura distribuída com vários clientes e servidores coordenados entre si.

## Arquitetura do Sistema

O sistema é composto por:
- **Broker:** Responsável por receber as requisições dos clientes (REQ/REP) e as distribuir entre os servidores em round-robin.
- **Proxy Pub/Sub:** Distribuição das mensagens aos clientes inscritos nos respectivos canais.
- **Servidores**: Processamento de requisições (login, canais, publicações) e gerenciamento de canais. Registram publicações e mantêm persistência dos dados em disco: conversam entre si para sincronizar o relógio, eleger um coordenador e replicar os dados.
- **Clientes/Bots:** Autenticação (login), consulta, criação e inscrição de canais. Publicam mensagens.

## Portas
| Componente | Porta |
|---|---|
| Broker (clientes, REQ → ROUTER) | 5555 |
| Broker (servidores, DEALER → REP) | 5556 |
| Proxy Pub/Sub (servidores publicam, XSUB) | 5557 |
| Proxy Pub/Sub (clientes e servidores assinam, XPUB) | 5558 |
| Servidores, entre si | 5560 + rank do servidor |
  
## Estrutura do Repositório

```text
.
├── cpp/                           # C++
│   ├── CMakeLists.txt             # gera o código do chat.proto e compila broker, proxy, servidor e cliente (ZeroMQ, Protobuf, threads)
│   ├── Dockerfile
│   └── src/
│       ├── broker.cpp             # broker (ROUTER 5555 <-> DEALER 5556)
│       ├── proxy.cpp              # proxy Pub/Sub (XSUB 5557 <-> XPUB 5558)
│       ├── server.cpp             # servidor: REP + PUB, réplica, relógio, eleição
│       ├── client.cpp             # bot (REQ + SUB)
│       └── common.hpp             # relógios, logs e utilidades compartilhadas
├── java/                          # Java
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/chat/java/   
│       ├── Server.java            # servidor: REP + PUB, réplica, relógio, eleição
│       ├── Client.java            # bot (REQ + SUB)
│       ├── Reference.java         # serviço de referência (rank, lista de servidores, heartbeat)
│       ├── ProxyPubSub.java       # proxy Pub/Sub 
│       └── Descricao.java         # texto das mensagens para o log
├── node/                          # Node.js (TypeScript)
│   ├── Dockerfile
│   ├── package.json
│   ├── package-lock.json
│   ├── tsconfig.json
│   └── src/
│       ├── server.ts              # servidor: REP + PUB, réplica, relógio, eleição
│       ├── client.ts              # bot (REQ + SUB)
│       ├── replica.ts             # replicação dos dados entre os servidores
│       └── common.ts              # relógios, logs e utilidades compartilhadas
├── proto/
│   └── chat.proto                 # contrato de mensagens, igual para todas as linguagens
├── scripts/                       # scripts de teste a partir do log do compose
│   └── check_parte2.py ... check_parte5.py
├── data/                          # dados dos servidores
├── docker-compose.yaml            # orquestração principal
├── docker-compose.host.yaml       # override opcional (rede do host, p/ ambientes bloqueados)
└── README.md                      # descrição do projeto
```
Nota: O broker e o proxy usados pelo `docker-compose.yaml` são os de C++ e atendem todas as linguagens.

## Funcionamento


### Parte 1: Login e canais
Os bots fazem login e criam e listam canais (`LoginRequest/Response`, `CreateChannelRequest/Response`, `ListChannelsRequest/Response`) por REQ/REP, passando pelo broker em round-robin. Um login com erro é repetido pelo bot a cada 2 s. Criar um canal duplicado retorna erro, e o bot só cria canais que não aparecem na listagem.

Na época, cada servidor tinha seu próprio conjunto de dados, então a listagem de canais variava conforme o servidor que respondia. Isso foi resolvido na Parte 5.

### Parte 2: Publicação e inscrição em canais
Os bots publicam com `PublishRequest{channel, message}` e recebem `PublishResponse`. O servidor grava a publicação e a envia ao tópico do canal (`Publication{channel, message, author}`), com dois frames: o nome do canal e o `Envelope`. O `timestamp_ms` é o horário de envio. O bot mede o de recebimento no próprio relógio e exibe os dois.

A inscrição é feita só no cliente: um único `SUB` assina vários tópicos. Como a inscrição do ZeroMQ casa por prefixo (`geral` também receberia `geral2`), o bot descarta o que não for exatamente de um canal em que se inscreveu. O `SUB` roda em uma thread própria, porque um socket ZeroMQ não pode ser usado por duas threads. Depois de se inscrever, o bot espera um instante para não perder a primeira mensagem (o "slow joiner" do ZeroMQ).

**Rotina padronizada dos bots:** Depois do login, a cada rodada:
1. lista os canais; se houver menos de 5, cria um novo;
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um, escolhido ao acaso;
3. escolhe um canal da lista e publica 10 mensagens aleatórias, 1 por segundo.

### Parte 3: Relógios e heartbeat
**Relógio lógico (Lamport).** Cada processo (bot e servidor) tem um contador protegido por mutex. Antes de enviar qualquer mensagem o contador é incrementado e seu valor vai em `Envelope.logical_clock`; ao receber (resposta ou publicação), `contador = max(contador, recebido)`. Todo log mostra `relogio=N`.

**Serviço de referência.** Os servidores falam com ele por REQ/REP:
- na partida, o servidor envia `RankRequest{name}` e guarda o rank (o mesmo nome mantém o mesmo rank);
- a cada 10 mensagens de clientes, envia `HeartbeatRequest{name}`; se a referência responder `success=false` (não conhece o servidor), ele se cadastra de novo;
- depois de um heartbeat bem-sucedido, pede e imprime a lista de servidores (`ListServersRequest`);
- a referência remove da lista o servidor que passa 30 s sem heartbeat.

Se a referência estiver fora do ar, o servidor continua atendendo os bots e tenta de novo nos próximos heartbeats. Nesta parte, a resposta do heartbeat trazia a hora, e o servidor sincronizava o relógio físico com ela (algoritmo de Cristian). Isso mudou na Parte 4.

**Sincronização do relógio físico (Cristian).** O servidor mede o RTT apenas entre o envio e o recebimento, estima a hora como `hora recebida + RTT/2` e calcula o deslocamento sobre o relógio bruto da máquina (não sobre o já corrigido, para não acumular erro). Descarta a estimativa se o RTT passar de 500 ms. Os timestamps nunca retrocedem.

### Parte 4: Coordenador, eleição e Berkeley
A referência deixa de devolver a hora no heartbeat (`time_ms` = 0). Um servidor é o **coordenador** e fornece a hora aos outros.

- **Sincronização:** a cada 15 mensagens de clientes, o servidor pede a hora ao coordenador (`ClockRequest` / `ClockResponse{time_ms}`) e aplica o mesmo cálculo de Cristian. Seguimos o diagrama do enunciado, em que o servidor pergunta a hora ao coordenador (uma forma simplificada do algoritmo de Berkeley).
- **Eleição (algoritmo do valentão, bully):** ao subir, ou quando o coordenador não responde, o servidor pede eleição (`ElectionRequest{name, rank}`) aos servidores de rank **maior**. Se algum responder OK (`ElectionResponse{ok}`), ele espera o anúncio; se ninguém responder, ele assume. Quem recebe o pedido de um servidor de rank menor responde OK e inicia a sua própria eleição. O de maior rank vence.
- **Anúncio:** o eleito publica `CoordinatorAnnouncement{name}` no tópico `servers`. Todos assinam esse tópico e guardam o nome do coordenador. O coordenador reforça o anúncio a cada ciclo de sincronização, porque quem entrou depois pode ter perdido o primeiro.
- **Convenção de endereços:** cada servidor escuta em `tcp://*:(5560 + rank)`. O endereço de um par é `tcp://<nome do servidor>:(5560 + rank do par)` (com `PEER_HOST=127.0.0.1` no compose host). O rank e a lista de servidores vêm da referência.
- **Falha do coordenador:** o pedido de hora fica sem resposta, o servidor inicia a eleição, e o de maior rank entre os que respondem assume e anuncia.

### Parte 5: Consistência e replicação
**Problema.** O broker distribui as requisições em round-robin, então cada servidor guardava só uma parte dos logins, canais e publicações. Se um servidor caísse, perdia-se uma parte do histórico. Requisito: todos os servidores devem ter todos os dados.

**Método escolhido:** Consistência eventual, com a ordem do histórico dada pelo relógio lógico.

- **Multi-mestre:** qualquer servidor aceita escritas dos clientes (round-robin do broker já faz). Não há réplica primária: a escrita continua com servidores caídos, e o coordenador da Parte 4 não vira gargalo nem ponto único de falha.
- **Propagação:** assim que um servidor grava um dado (login, criação de canal ou publicação), ele o envia aos outros (`ReplicateRequest` / `ReplicateResponse`). A escrita do cliente não espera a propagação.
- **Anti-entropia:** a propagação pode falhar (servidor fora do ar, mensagem perdida). A cada 10 s, cada servidor compara o que tem com o de cada par (`SyncRequest` com a "versão": até que sequência tem de cada origem) e busca o que falta (`SyncResponse`, em lotes). Isso também faz um servidor que entra ou volta (inclusive com o disco vazio) recuperar todo o histórico.
- **Registros idempotentes:** cada dado vira um `ReplicaRecord` com id único `(origin, seq)`, em que `origin` é `<servidor>@<instante de partida>`. Aplicar o mesmo registro duas vezes não muda nada, então propagação e anti-entropia podem se sobrepor sem duplicar dados. Um servidor que reinicia sem disco usa uma origem nova e nunca reaproveita ids antigos.
- **Ordem do histórico:** cada registro guarda o relógio lógico do momento da escrita; o histórico é ordenado por (relógio lógico, servidor de origem), a mesma ordem em todos os servidores.
- **Só quem aceitou publica:** um registro replicado é apenas guardado, não é republicado no tópico, para os assinantes não receberem a mesma mensagem duas vezes.

## Formato das mensagens

Todas as mensagens utilizam um Envelope, definido em proto/chat.proto:

- timestamp_ms: horário de envio.
- sender: identificação do remetente.
- logical_clock: contador lógico do processo.
- body: conteúdo da mensagem, definido por meio de oneof.

Os tipos de mensagens incluem requisições e respostas de login, gerenciamento de canais, publicação, heartbeat e sincronização de relógios.

### Formato do log

Todos os processos registram as mensagens no mesmo formato, que os scripts de `scripts/` leem:

```text
<hora> | <quem> | ENVIADO/RECEBIDO/PUBLICADO | de=<remetente> | relogio=<N> | <conteúdo>
```

Quando o bot recebe uma publicação, mostra o canal, os dois relógios, o autor e os horários de envio e de recebimento.

## Persistência

Cada servidor possui seu próprio volume de dados, armazenando:

- `logins.tsv`: registros de login (`timestamp_ms<TAB>usuário`).
- `channels.txt`: canais criados, um por linha.
- `publications.bin`: publicações (canal, autor, texto, timestamp e relógio lógico), no mesmo formato de 4 bytes + `Envelope`.

Para listar as publicações em ordem (relógio lógico e, no empate, servidor de origem):
```bash
docker compose exec server_cpp_1 /app/server --dump /data/publications.bin
```

Os servidores Java e Node também gravam em seus volumes `/data`. O compose usa volumes nomeados no C++ e no Node, e a pasta `./data/java1` e `./data/java2` no Java.

Os dados são carregados na inicialização dos servidores.

## Como executar
Para construir as imagens e iniciar os serviços:
```bash
docker compose up --build
```

### Ambientes com rede bridge bloqueada (ex.: alguns GitHub Codespaces)
Em alguns ambientes, a comunicação entre containers pela rede bridge pode apresentar problemas. Nesses casos, utilize a configuração alternativa de rede do host (os endereços passam a ser `127.0.0.1`):
```bash
docker compose -f docker-compose.yaml -f docker-compose.host.yaml up --build
```

### Scripts de Teste

Os scripts em `scripts/` leem o log do compose e conferem os requisitos de cada parte. Para executar, basta:
```bash
docker compose -f docker-compose.yaml -f docker-compose.host.yaml up -d --build
sleep 90
docker compose -f docker-compose.yaml -f docker-compose.host.yaml logs --no-color > run.log
docker compose -f docker-compose.yaml -f docker-compose.host.yaml down
python3 scripts/check_parte4.py run.log
```
Para a Parte 5, os bots precisam parar antes de coletar o log, para a réplica convergir; o passo a passo está no cabeçalho de `scripts/check_parte5.py`. A remoção de um servidor parado e a queda do coordenador são testadas à mão com `docker compose stop <servidor>`.
- **Canais:** `list_channels` devolve os canais de todos os servidores, e criar um canal confere se ele já existe em qualquer servidor.
- **Log:** a cada ciclo, o servidor imprime `réplica: registros=N (logins=a, canais=b, publicações=c) hash=H`. O hash resume o conjunto de ids: servidores com os mesmos registros têm o mesmo hash.
Sincronização do relógio físico (Cristian) — a resposta do heartbeat traz ```time_ms```. O servidor mede o RTT apenas entre o envio e o recebimento, estima e calcula o deslocamento a partir do relógio bruto da máquina e o aplica (descarta se RTT > 500 ms). Os timestamps nunca retrocedem.
