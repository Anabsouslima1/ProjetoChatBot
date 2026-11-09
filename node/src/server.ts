// Servidor (REP + PUB + REQ para a referência): trata login, canais e publicações.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv        -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt      -> um canal por linha
//   publications.bin  -> cada publicação: 4 bytes (big-endian) com o tamanho + Envelope serializado
//                        (protobuf). Contém canal, autor, texto e timestamp. Mesmo formato do servidor C++.
// Para listar o conteúdo de publications.bin:  tsx src/server.ts --dump /data/publications.bin
//
// Parte 3: relógio lógico (Lamport) em todas as mensagens; cadastro na referência (rank);
// a cada HEARTBEAT_EVERY (10) mensagens de clientes: heartbeat + hora correta (Cristian) + lista de servidores.
import { Reply, Publisher, Request } from "zeromq";
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
// Mesmo nome de variável usado pelos servidores Java e C++ (REFERENCE_ADDR); REFERENCE_ENDPOINT também vale.
const referenceEp = envOr("REFERENCE_ADDR", envOr("REFERENCE_ENDPOINT", "tcp://reference:5559"));
const dataDir = envOr("DATA_DIR", "/data");
// Enunciado: heartbeat a cada 10 mensagens de clientes (ajustável só para testes)
const heartbeatEvery = Math.max(1, envInt("HEARTBEAT_EVERY", 10));

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
// Cliente do serviço de referência (REQ/REP). Se a referência não responde, o socket REQ é
// recriado (lazy pirate) e o servidor continua atendendo os bots normalmente.
// ---------------------------------------------------------------------------
const MAX_SYNC_RTT_MS = 500; // ida e volta maior que isso: estimativa ruim, descarta a sincronização

class ReferenceClient {
  private sock!: Request;

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
    const r = await this.call({ rankRequest: { name: this.who } });
    if (!r || r.rep.body !== "rankResponse") return null;
    return r.rep.rankResponse.rank;
  }

  // REQ: heartbeat -> REP: OK + hora correta. Retorna 1 = ok (relógio ajustado),
  // 0 = sem resposta válida, -1 = a referência não conhece este servidor (success=false).
  async heartbeat(): Promise<number> {
    const r = await this.call({ heartbeatRequest: { name: this.who } });
    if (!r || r.rep.body !== "heartbeatResponse") return 0;
    const h = r.rep.heartbeatResponse;
    if (!h.success) return -1;
    this.syncClock(h.timeMs, r.rttMs, r.localAtReceive);
    return 1;
  }

  // REQ: list -> REP: [nomes e ranks] (o conteúdo aparece no log da resposta)
  async listServers(): Promise<boolean> {
    const r = await this.call({ listServersRequest: {} });
    return r !== null && r.rep.body === "listServersResponse";
  }

  // Algoritmo de Cristian: a hora da referência chegou depois de ~metade do tempo de ida e volta,
  // então a hora correta agora é (hora da referência + rtt/2). O deslocamento é calculado sobre a
  // hora ORIGINAL da máquina (nowMs), nunca sobre a já corrigida, para não acumular erro.
  private syncClock(referenceMs: number, rttMs: number, localAtReceive: number): void {
    if (rttMs > MAX_SYNC_RTT_MS) {
      console.log(
        `[${this.who}] sincronização descartada: ida e volta de ${rttMs.toFixed(1)} ms é grande demais para uma boa estimativa`,
      );
      return;
    }
    const estimated = referenceMs + Math.round(rttMs / 2);
    const newOffset = estimated - localAtReceive;
    const oldOffset = getClockOffset();
    setClockOffset(newOffset);
    console.log(
      `[${this.who}] relógio sincronizado: deslocamento ${oldOffset} ms -> ${newOffset} ms (ida e volta: ${rttMs.toFixed(1)} ms)`,
    );
  }

  // Envia a requisição e espera a resposta. rttMs / localAtReceive medem o tempo entre o send e o
  // recv e a hora da máquina no instante do recv, sem incluir serialização nem log.
  private async call(
    body: Record<string, unknown>,
  ): Promise<{ rep: Envelope; rttMs: number; localAtReceive: number } | null> {
    const req = makeEnvelope(this.who, body);
    stamp(req); // hora + relógio lógico incrementado, imediatamente antes de enviar
    const payload = encode(req);
    logMsg(this.who, "ENVIADO", req);

    const t0 = performance.now();
    try {
      await this.sock.send(payload);
      const [raw] = await this.sock.receive();
      const t1 = performance.now();
      const localAtReceive = nowMs();
      const rep = decode(raw);
      logicalClock.observe(rep.logicalClock); // relógio lógico: max(local, recebido)
      logMsg(this.who, "RECEBIDO", rep);
      return { rep, rttMs: t1 - t0, localAtReceive };
    } catch {
      console.log(`[${this.who}] referência sem resposta em ${this.endpoint}, recriando socket`);
      this.reset();
      return null;
    }
  }
}

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
      // Mensagem que vai para o tópico: carimbada (hora + relógio lógico) logo antes de
      // gravar e publicar; autor = bot que pediu.
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
  `[${name}] conectado ao broker em ${backend} e ao proxy Pub/Sub em ${pubsub} | referência em ${referenceEp} | dados em ${dataDir} | canais carregados: ${channels.size}`,
);

// --- Parte 3: cadastro na referência (rank) ---
const ref = new ReferenceClient(referenceEp, name);
let registered = false;
let rank = 0;

async function tryRegister(): Promise<void> {
  const r = await ref.registerServer();
  if (r !== null) {
    rank = r;
    registered = true;
    console.log(`[${name}] rank recebido da referência: ${rank}`);
  }
}

// Poucas tentativas na partida (o servidor não deve ficar parado esperando a referência);
// se não der, tenta de novo nos próximos heartbeats.
for (let attempt = 1; attempt <= 5 && !registered; attempt++) {
  await tryRegister();
  if (!registered) await sleep(500);
}
if (registered) {
  await ref.listServers();
} else {
  console.log(`[${name}] referência indisponível; atendendo sem rank (nova tentativa nos próximos heartbeats)`);
}

let sinceHeartbeat = 0;

// A cada heartbeatEvery mensagens de clientes: heartbeat (+ hora correta) e lista de servidores.
async function afterClientMessage(): Promise<void> {
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
  if (referenceOk) await ref.listServers();
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
    await afterClientMessage(); // heartbeat depois de responder ao bot
  } catch (e) {
    console.error(`[${name}] erro no heartbeat:`, e);
  }
}
