// Servidor (REP + PUB): trata login, canais e publicações.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv        -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt      -> um canal por linha
//   publications.bin  -> cada publicação: 4 bytes (big-endian) com o tamanho + Envelope serializado
//                        (protobuf). Contém canal, autor, texto e timestamp. Mesmo formato do servidor C++.
// Para listar o conteúdo de publications.bin:  tsx src/server.ts --dump /data/publications.bin
import { Reply, Publisher } from "zeromq";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  MAX_MESSAGE_LEN,
  decode,
  encode,
  envOr,
  logMsg,
  makeEnvelope,
  nowMs,
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
const dataDir = envOr("DATA_DIR", "/data");

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
        appendFileSync(loginsFile, `${nowMs()}\t${user}\n`);
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
      // Mensagem que vai para o tópico: timestamp do envio da publicação, autor = bot.
      const pubmsg = makeEnvelope(name, {
        publication: { channel: ch, message: text, author: req.sender },
      });
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
  `[${name}] conectado ao broker em ${backend} e ao proxy Pub/Sub em ${pubsub} | dados em ${dataDir} | canais carregados: ${channels.size}`,
);

for await (const [msg] of sock) {
  let rep: Envelope;
  try {
    const req = decode(msg);
    logMsg(name, "RECEBIDO", req);
    rep = await handle(req);
  } catch (e) {
    console.error(`[${name}] mensagem inválida recebida:`, e);
    rep = makeEnvelope(name, {
      loginResponse: fail("ERROR_UNKNOWN_REQUEST", "mensagem não é um Envelope válido"),
    });
  }
  rep.timestampMs = nowMs();
  // REP precisa sempre responder, senão o socket trava
  await sock.send(encode(rep));
  logMsg(name, "ENVIADO", rep);
}