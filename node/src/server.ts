// Servidor (REP + PUB + REQ para a referência): login, canais, publicações, relógios, heartbeat e eleição.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv        -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt      -> um canal por linha
//   publications.bin  -> cada publicação: 4 bytes (big-endian) com o tamanho + Envelope serializado
//                        (protobuf). Mesmo formato do servidor C++.
// Para listar o conteúdo de publications.bin:  tsx src/server.ts --dump /data/publications.bin
//
// Parte 3: relógio lógico (Lamport); cadastro na referência (rank);
//          a cada HEARTBEAT_EVERY (10) mensagens de clientes: heartbeat + lista de servidores.
//
// Parte 4 (a hora deixa de vir da referência):
//   - um servidor é o coordenador (Bully: vence o maior rank);
//   - a cada CLOCK_SYNC_EVERY (15) mensagens de clientes, pede a hora ao coordenador
//     (clock_request / clock_response) e ajusta o relógio físico (Cristian: hora + rtt/2);
//   - se o coordenador não responde (ou não há um), inicia eleição: pede eleição aos servidores de
//     rank maior (election_request / OK); se ninguém responde, assume e publica no tópico "servers";
//   - cada servidor atende os outros em tcp://*:(PEER_PORT_BASE + rank). O endereço de um par é
//     tcp://<PEER_HOST ou nome do servidor>:(PEER_PORT_BASE + rank do par).
import { Reply, Publisher, Request, Subscriber } from "zeromq";
import { setTimeout as sleep } from "node:timers/promises";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  MAX_MESSAGE_LEN,
  clockMs,
  decode,
  encode,
  envInt,
  envOr,
  getClockOffset,
  logMsg,
  logicalClock,
  makeEnvelope,
  nowMs,
  setClockOffset,
  stamp,
  validMessage,
  validName,
} from "./common.js";
import type { Envelope } from "./common.js";

// Lê publications.bin e imprime o conteúdo de cada registro.
function dumpPublications(path: string): number {
  if (!existsSync(path)) {
    console.error(`não foi possível abrir ${path}`);
    return 1;
  }
  const buf = readFileSync(path);
  let off = 0;
  let count = 0;
  while (off < buf.length) {
    if (off + 4 > buf.length) {
      console.error("registro truncado no final do arquivo");
      return 1;
    }
    const n = buf.readUInt32BE(off);
    off += 4;
    if (off + n > buf.length) {
      console.error("registro truncado no final do arquivo");
      return 1;
    }
    logMsg("dump", "ARMAZENADO", decode(buf.subarray(off, off + n)));
    off += n;
    count++;
  }
  console.log(`${count} publicação(ões) no arquivo`);
  return 0;
}

if (process.argv[2] === "--dump" && process.argv[3]) {
  process.exit(dumpPublications(process.argv[3]));
}

const name = envOr("SERVER_NAME", "server_node");
const backend = envOr("BROKER_BACKEND", "tcp://broker:5556");
const pubsub = envOr("PROXY_XSUB", "tcp://proxy:5557");
const xpub = envOr("PROXY_XPUB", "tcp://proxy:5558");
// Mesmo nome de variável usado pelos servidores Java e C++ (REFERENCE_ADDR); REFERENCE_ENDPOINT também vale.
const referenceEp = envOr("REFERENCE_ADDR", envOr("REFERENCE_ENDPOINT", "tcp://reference:5559"));
const dataDir = envOr("DATA_DIR", "/data");
// Enunciado: heartbeat a cada 10 mensagens de clientes; relógio a cada 15 (ajustáveis só para testes)
const heartbeatEvery = Math.max(1, envInt("HEARTBEAT_EVERY", 10));
const clockSyncEvery = Math.max(1, envInt("CLOCK_SYNC_EVERY", 15));
const peerPortBase = envInt("PEER_PORT_BASE", 5560);

mkdirSync(dataDir, { recursive: true });
const channelsFile = join(dataDir, "channels.txt");
const loginsFile = join(dataDir, "logins.tsv");
const publicationsFile = join(dataDir, "publications.bin");

const channels = new Set<string>(
  existsSync(channelsFile)
    ? readFileSync(channelsFile, "utf8").split("\n").filter((l) => l.length > 0)
    : [],
);

function fail(error: string, errorMessage: string) {
  return { success: false, error, errorMessage };
}
const ok = { success: true, error: "ERROR_NONE", errorMessage: "" };

// Grava a publicação (Envelope serializado) com o tamanho de 4 bytes na frente
function addPublication(payload: Uint8Array): void {
  const hdr = Buffer.alloc(4);
  hdr.writeUInt32BE(payload.length);
  appendFileSync(publicationsFile, Buffer.concat([hdr, payload]));
}

// ---------------------------------------------------------------------------
// Algoritmo de Cristian: a hora de quem respondeu chegou depois de ~metade do tempo de ida e volta,
// então a hora correta agora é (hora remota + rtt/2). O deslocamento é calculado sobre a hora
// ORIGINAL da máquina (nowMs), nunca sobre a já corrigida, para não acumular erro.
// ---------------------------------------------------------------------------
const MAX_SYNC_RTT_MS = 500; // ida e volta maior que isso: estimativa ruim, descarta a sincronização

function applySync(from: string, remoteMs: number, rttMs: number, localAtReceive: number): void {
  if (rttMs > MAX_SYNC_RTT_MS) {
    console.log(
      `[${name}] sincronização descartada: ida e volta de ${rttMs.toFixed(1)} ms é grande demais para uma boa estimativa`,
    );
    return;
  }
  const estimated = remoteMs + Math.round(rttMs / 2);
  const newOffset = estimated - localAtReceive;
  const oldOffset = getClockOffset();
  setClockOffset(newOffset);
  console.log(
    `[${name}] relógio sincronizado com ${from}: deslocamento ${oldOffset} ms -> ${newOffset} ms (ida e volta: ${rttMs.toFixed(1)} ms)`,
  );
}

// ---------------------------------------------------------------------------
// Cliente do serviço de referência (REQ/REP). Se a referência não responde, o socket REQ é
// recriado (lazy pirate) e o servidor continua atendendo os bots normalmente.
// As chamadas são enfileiradas (um REQ não pode ter duas requisições ao mesmo tempo).
// ---------------------------------------------------------------------------
type Peer = { name: string; rank: number };

class ReferenceClient {
  private sock!: Request;
  private chain: Promise<unknown> = Promise.resolve();

  constructor(private endpoint: string, private who: string) {
    this.reset();
  }

  private reset(): void {
    this.sock?.close();
    this.sock = new Request({ linger: 0, receiveTimeout: 1500, sendTimeout: 1500 });
    this.sock.connect(this.endpoint);
  }

  // REQ: nome -> REP: rank (isso também cadastra o servidor na referência)
  async registerServer(): Promise<number | null> {
    const rep = await this.call({ rankRequest: { name: this.who } });
    if (!rep || rep.body !== "rankResponse") return null;
    return rep.rankResponse.rank;
  }

  // REQ: heartbeat -> REP: OK. Retorna 1 = ok, 0 = sem resposta válida,
  // -1 = a referência não conhece este servidor (success=false).
  // (Parte 4: a hora não vem mais na resposta do heartbeat.)
  async heartbeat(): Promise<number> {
    const rep = await this.call({ heartbeatRequest: { name: this.who } });
    if (!rep || rep.body !== "heartbeatResponse") return 0;
    return rep.heartbeatResponse.success ? 1 : -1;
  }

  // REQ: list -> REP: [nomes e ranks]
  async listServers(): Promise<Peer[] | null> {
    const rep = await this.call({ listServersRequest: {} });
    if (!rep || rep.body !== "listServersResponse") return null;
    return (rep.listServersResponse.servers as any[]).map((s) => ({ name: s.name, rank: s.rank }));
  }

  private call(body: Record<string, unknown>): Promise<Envelope | null> {
    const run = this.chain.then(() => this.doCall(body));
    this.chain = run.catch(() => {});
    return run;
  }

  private async doCall(body: Record<string, unknown>): Promise<Envelope | null> {
    const req = makeEnvelope(this.who, body);
    stamp(req); // hora + relógio lógico incrementado, imediatamente antes de enviar
    const payload = encode(req);
    logMsg(this.who, "ENVIADO", req);
    try {
      await this.sock.send(payload);
      const [raw] = await this.sock.receive();
      const rep = decode(raw);
      logicalClock.observe(rep.logicalClock); // relógio lógico: max(local, recebido)
      logMsg(this.who, "RECEBIDO", rep);
      return rep;
    } catch {
      console.log(`[${this.who}] referência sem resposta em ${this.endpoint}, recriando socket`);
      this.reset();
      return null;
    }
  }
}

// ---------------------------------------------------------------------------
// Parte 4: estado do cluster, comunicação entre servidores
// ---------------------------------------------------------------------------
let registered = false;
let rank = 0;
let coordinator = ""; // vazio = ainda não há coordenador conhecido
let peers: Peer[] = []; // todos os servidores (nome, rank)
let electionWanted = false; // um servidor de rank menor pediu eleição
let waitUntilMs = 0; // após perder uma eleição, espera o anúncio até aqui
let electionRunning = false;

const steadyMs = () => performance.now();
const rankOf = (n: string): number => peers.find((p) => p.name === n)?.rank ?? -1;
const peerEndpoint = (serverName: string, r: number) =>
  `tcp://${envOr("PEER_HOST", serverName)}:${peerPortBase + r}`;

// REQ avulso a outro servidor (um socket novo por chamada: se o par caiu, nada fica travado).
// O RTT é medido só entre o send e o recv.
async function peerRequest(
  ep: string,
  body: Record<string, unknown>,
  timeoutMs: number,
): Promise<{ rep: Envelope; rttMs: number; localAtReceive: number } | null> {
  const req = makeEnvelope(name, body);
  stamp(req);
  const payload = encode(req);
  logMsg(name, "ENVIADO", req);
  const s = new Request({ linger: 0, receiveTimeout: timeoutMs, sendTimeout: timeoutMs });
  try {
    s.connect(ep);
    const t0 = performance.now();
    await s.send(payload);
    const [raw] = await s.receive();
    const t1 = performance.now();
    const localAtReceive = nowMs();
    const rep = decode(raw);
    logicalClock.observe(rep.logicalClock);
    logMsg(name, "RECEBIDO", rep);
    return { rep, rttMs: t1 - t0, localAtReceive };
  } catch (e) {
    console.log(`[${name}] sem resposta de ${ep}: ${(e as Error).message}`);
    return null;
  } finally {
    s.close();
  }
}

// Pede eleição a todos os servidores de rank maior ao mesmo tempo. true se algum respondeu OK.
function askHigherServers(higher: Peer[], timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    let pending = higher.length;
    for (const h of higher) {
      peerRequest(
        peerEndpoint(h.name, h.rank),
        { electionRequest: { name, rank } },
        timeoutMs,
      ).then((r) => {
        if (r && r.rep.body === "electionResponse" && r.rep.electionResponse.ok) resolve(true);
        else if (--pending === 0) resolve(false);
      });
    }
  });
}

// Atende os pedidos dos outros servidores (clock_request e election_request).
async function peerServerLoop(): Promise<void> {
  const bind = `tcp://*:${peerPortBase + rank}`;
  const rep = new Reply({ linger: 0 });
  try {
    await rep.bind(bind);
  } catch (e) {
    console.log(`[${name}] não foi possível escutar em ${bind}: ${(e as Error).message}`);
    return;
  }
  console.log(`[${name}] atendendo outros servidores em ${bind}`);
  for await (const [msg] of rep) {
    let resp: Envelope;
    try {
      const req = decode(msg);
      logicalClock.observe(req.logicalClock);
      logMsg(name, "RECEBIDO", req);
      if (req.body === "clockRequest") {
        resp = makeEnvelope(name, { clockResponse: { timeMs: 0 } });
      } else if (req.body === "electionRequest") {
        // "OK" só se quem pede tem rank menor; neste caso eu assumo a eleição
        const okElection = req.electionRequest.rank < rank;
        resp = makeEnvelope(name, { electionResponse: { ok: okElection } });
        if (okElection) electionWanted = true;
      } else {
        resp = makeEnvelope(name, { electionResponse: { ok: false } });
      }
    } catch {
      resp = makeEnvelope(name, { electionResponse: { ok: false } });
    }
    stamp(resp);
    // hora enviada = a mesma do carimbo da resposta (hora do coordenador no momento do envio)
    if (resp.clockResponse !== undefined) resp.clockResponse.timeMs = resp.timestampMs;
    await rep.send(encode(resp)); // REP precisa sempre responder
    logMsg(name, "ENVIADO", resp);
  }
}

// Assina o tópico "servers" (proxy XPUB) e guarda o coordenador anunciado.
async function coordinatorListener(): Promise<void> {
  const sub = new Subscriber({ linger: 0 });
  sub.connect(xpub);
  sub.subscribe("servers");
  for await (const [topic, body] of sub) {
    // A inscrição casa por prefixo: só aceitamos o tópico exato.
    if (topic.toString() !== "servers") continue;
    try {
      const e = decode(body);
      if (e.body !== "coordinatorAnnouncement") continue;
      logicalClock.observe(e.logicalClock);
      logMsg(name, "RECEBIDO", e);
      const coord: string = e.coordinatorAnnouncement.name;
      if (coord === name) continue;
      // Alguém de rank menor se anunciou enquanto eu sou o coordenador: contesto (volta a haver eleição).
      if (coordinator === name && rankOf(coord) >= 0 && rankOf(coord) < rank) {
        electionWanted = true;
        continue;
      }
      coordinator = coord;
      console.log(`[${name}] coordenador atual: ${coord}`);
    } catch {
      // mensagem inválida: ignora
    }
  }
}

// ---------------------------------------------------------------------------
// Sockets principais
// ---------------------------------------------------------------------------
const sock = new Reply(); // requisições dos bots (via broker)
sock.connect(backend);
const pub = new Publisher(); // publicações nos canais (via proxy)
pub.connect(pubsub);

async function handle(req: Envelope): Promise<Envelope> {
  switch (req.body) {
    case "loginRequest": {
      const user: string = req.loginRequest.username;
      if (!validName(user)) {
        return makeEnvelope(name, {
          loginResponse: fail("ERROR_INVALID_USERNAME", "usuário deve ter 1-32 caracteres [A-Za-z0-9_-]"),
        });
      }
      try {
        appendFileSync(loginsFile, `${clockMs()}\t${user}\n`);
      } catch {
        return makeEnvelope(name, {
          loginResponse: fail("ERROR_INTERNAL", "falha ao gravar login em disco"),
        });
      }
      return makeEnvelope(name, { loginResponse: ok });
    }

    case "createChannelRequest": {
      const ch: string = req.createChannelRequest.channelName;
      if (!validName(ch)) {
        return makeEnvelope(name, {
          createChannelResponse: fail("ERROR_INVALID_CHANNEL_NAME", "canal deve ter 1-32 caracteres [A-Za-z0-9_-]"),
        });
      }
      if (channels.has(ch)) {
        return makeEnvelope(name, {
          createChannelResponse: fail("ERROR_CHANNEL_EXISTS", "canal já existe"),
        });
      }
      try {
        appendFileSync(channelsFile, `${ch}\n`);
      } catch {
        return makeEnvelope(name, {
          createChannelResponse: fail("ERROR_INTERNAL", "falha ao gravar canal em disco"),
        });
      }
      channels.add(ch);
      return makeEnvelope(name, { createChannelResponse: ok });
    }

    case "listChannelsRequest":
      return makeEnvelope(name, { listChannelsResponse: { channels: [...channels] } });

    case "publishRequest": {
      const ch: string = req.publishRequest.channel;
      const text: string = req.publishRequest.message;
      // Cada servidor conhece só os canais criados nele (dados separados), então não exigimos
      // que o canal exista aqui: basta o nome ser válido (o tópico é só o nome).
      if (!validName(ch)) {
        return makeEnvelope(name, {
          publishResponse: fail("ERROR_INVALID_CHANNEL_NAME", "canal deve ter 1-32 caracteres [A-Za-z0-9_-]"),
        });
      }
      if (!validMessage(text)) {
        return makeEnvelope(name, {
          publishResponse: fail("ERROR_INVALID_MESSAGE", `mensagem deve ter 1-${MAX_MESSAGE_LEN} bytes`),
        });
      }
      const pubmsg = makeEnvelope(name, {
        publication: { channel: ch, message: text, author: req.sender },
      });
      stamp(pubmsg);
      const payload = encode(pubmsg);
      try {
        addPublication(payload);
      } catch {
        return makeEnvelope(name, {
          publishResponse: fail("ERROR_INTERNAL", "falha ao gravar publicação em disco"),
        });
      }
      // PUB: frame 1 = tópico (nome do canal), frame 2 = Envelope
      await pub.send([ch, payload]);
      logMsg(name, "PUBLICADO", pubmsg);
      return makeEnvelope(name, { publishResponse: ok });
    }

    default:
      return makeEnvelope(name, {
        loginResponse: fail("ERROR_UNKNOWN_REQUEST", "tipo de requisição não suportado"),
      });
  }
}

console.log(
  `[${name}] conectado ao broker em ${backend} e ao proxy Pub/Sub em ${pubsub} (XPUB ${xpub}) | referência em ${referenceEp} | dados em ${dataDir} | canais carregados: ${channels.size}`,
);

// --- Parte 3: cadastro na referência (rank) ---
const ref = new ReferenceClient(referenceEp, name);
let peerThreadStarted = false;

async function refreshPeers(): Promise<boolean> {
  const v = await ref.listServers();
  if (v === null) return false;
  peers = v;
  return true;
}

async function tryRegister(): Promise<void> {
  const r = await ref.registerServer();
  if (r === null) return;
  rank = r;
  registered = true;
  console.log(`[${name}] rank recebido da referência: ${rank}`);
  if (!peerThreadStarted) {
    // passa a atender os outros servidores em PEER_PORT_BASE + rank
    peerThreadStarted = true;
    peerServerLoop().catch((e) => console.error(`[${name}] erro no atendimento a pares:`, e));
  }
}

coordinatorListener().catch((e) => console.error(`[${name}] erro no SUB de "servers":`, e));

// Poucas tentativas na partida (o servidor não deve ficar parado esperando a referência);
// se não der, tenta de novo nos próximos heartbeats.
for (let attempt = 1; attempt <= 5 && !registered; attempt++) {
  await tryRegister();
  if (!registered) await sleep(500);
}
if (registered) {
  await refreshPeers();
} else {
  console.log(`[${name}] referência indisponível; atendendo sem rank (nova tentativa nos próximos heartbeats)`);
}

// --- Parte 4: coordenador, eleição e sincronização ---

// Publica no tópico "servers" que este servidor é o coordenador.
async function announce(): Promise<void> {
  const e = makeEnvelope(name, { coordinatorAnnouncement: { name } });
  stamp(e);
  await pub.send(["servers", encode(e)]);
  logMsg(name, "PUBLICADO", e);
}

// Bully: pede eleição aos servidores de rank maior; se nenhum responde OK, este servidor assume.
async function runElection(): Promise<void> {
  if (electionRunning) return;
  electionRunning = true;
  try {
    await refreshPeers();
    const higher = peers.filter((p) => p.rank > rank);
    console.log(
      `[${name}] eleição iniciada (rank ${rank}): consultando ${higher.length} servidor(es) de rank maior`,
    );
    if (higher.length > 0 && (await askHigherServers(higher, 1000))) {
      coordinator = "";
      waitUntilMs = steadyMs() + 4000; // alguém de rank maior assume e vai anunciar
      console.log(`[${name}] um servidor de rank maior respondeu OK; aguardando o anúncio do coordenador`);
    } else {
      coordinator = name;
      console.log(`[${name}] eleito coordenador`);
      await announce();
    }
  } finally {
    electionRunning = false;
  }
}

// Eleição pedida por outro servidor, ou ainda sem coordenador.
async function maintain(): Promise<void> {
  if (!registered || electionRunning) return;
  const wanted = electionWanted;
  electionWanted = false;
  const none = coordinator === "" && steadyMs() >= waitUntilMs;
  if (wanted || none) await runElection();
}

// Sem clientes o laço principal fica parado esperando mensagem; este timer garante que as
// eleições continuem acontecendo (equivale ao rcvtimeo de 200 ms do servidor C++).
setInterval(() => {
  maintain().catch((e) => console.error(`[${name}] erro na manutenção:`, e));
}, 200);

// A cada clockSyncEvery mensagens: pede a hora ao coordenador.
async function syncWithCoordinator(): Promise<void> {
  if (!registered) return;
  const coord = coordinator;
  if (coord === "") return; // maintain() cuida da eleição
  if (coord === name) {
    await announce(); // reforça o anúncio (quem entrou depois pode não ter recebido o primeiro)
    return;
  }
  let crank = rankOf(coord);
  if (crank < 0) {
    await refreshPeers();
    crank = rankOf(coord);
  }
  const r = crank >= 0 ? await peerRequest(peerEndpoint(coord, crank), { clockRequest: {} }, 1000) : null;
  if (r && r.rep.body === "clockResponse") {
    applySync(coord, r.rep.clockResponse.timeMs, r.rttMs, r.localAtReceive);
  } else {
    console.log(`[${name}] coordenador ${coord} não respondeu; iniciando eleição`);
    coordinator = "";
    await runElection();
  }
}

let sinceHeartbeat = 0;
let sinceSync = 0;

// A cada heartbeatEvery mensagens de clientes: heartbeat e lista de servidores.
// A cada clockSyncEvery: sincronização com o coordenador.
async function afterClientMessage(): Promise<void> {
  if (++sinceSync >= clockSyncEvery) {
    sinceSync = 0;
    await syncWithCoordinator();
  }
  if (++sinceHeartbeat < heartbeatEvery) return;
  sinceHeartbeat = 0;
  let referenceOk = false; // só pede a lista se a referência respondeu agora
  if (!registered) {
    await tryRegister();
    referenceOk = registered;
  } else {
    const hb = await ref.heartbeat();
    if (hb === 1) {
      referenceOk = true;
    } else if (hb === -1) {
      console.log(`[${name}] a referência não conhece este servidor; cadastrando de novo`);
      registered = false;
      await tryRegister();
      referenceOk = registered;
    }
  }
  if (referenceOk) await refreshPeers();
}

for await (const [msg] of sock) {
  let rep: Envelope;
  try {
    const req = decode(msg);
    logicalClock.observe(req.logicalClock); // relógio lógico: max(local, recebido)
    logMsg(name, "RECEBIDO", req);
    rep = await handle(req);
  } catch (e) {
    console.error(`[${name}] mensagem inválida recebida:`, e);
    rep = makeEnvelope(name, {
      loginResponse: fail("ERROR_UNKNOWN_REQUEST", "mensagem não é um Envelope válido"),
    });
  }
  stamp(rep); // hora + relógio lógico incrementado, imediatamente antes de enviar
  // REP precisa sempre responder, senão o socket trava
  await sock.send(encode(rep));
  logMsg(name, "ENVIADO", rep);

  try {
    await afterClientMessage(); // sincronização/heartbeat depois de responder ao bot
    await maintain();
  } catch (e) {
    console.error(`[${name}] erro no pós-processamento:`, e);
  }
}