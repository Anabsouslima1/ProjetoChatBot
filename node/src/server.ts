// Servidor (REP): trata login, criação e listagem de canais.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv   -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt -> um canal por linha
import { Reply } from "zeromq";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { decode, encode, envOr, logMsg, makeEnvelope, nowMs, validName } from "./common.js";
import type { Envelope } from "./common.js";

const name = envOr("SERVER_NAME", "server_node");
const backend = envOr("BROKER_BACKEND", "tcp://broker:5556");
const dataDir = envOr("DATA_DIR", "/data");

mkdirSync(dataDir, { recursive: true });
const channelsFile = join(dataDir, "channels.txt");
const loginsFile = join(dataDir, "logins.tsv");

const channels = new Set<string>(
  existsSync(channelsFile)
    ? readFileSync(channelsFile, "utf8").split("\n").filter((l) => l.length > 0)
    : [],
);

function fail(error: string, errorMessage: string) {
  return { success: false, error, errorMessage };
}
const ok = { success: true, error: "ERROR_NONE", errorMessage: "" };

function handle(req: Envelope): Envelope {
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

    default:
      return makeEnvelope(name, {
        loginResponse: fail("ERROR_UNKNOWN_REQUEST", "tipo de requisição não suportado"),
      });
  }
}

const sock = new Reply();
sock.connect(backend);
console.log(`[${name}] conectado ao broker em ${backend} | dados em ${dataDir} | canais carregados: ${channels.size}`);

for await (const [msg] of sock) {
  let rep: Envelope;
  try {
    const req = decode(msg);
    logMsg(name, "RECEBIDO", req);
    rep = handle(req);
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