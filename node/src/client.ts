// Cliente (bot): faz login, lista canais e cria os que ainda não existem.
import { Request } from "zeromq";
import { setTimeout as sleep } from "node:timers/promises";
import { decode, encode, envOr, logMsg, makeEnvelope } from "./common.js";
import type { Envelope } from "./common.js";

const name = envOr("BOT_NAME", "bot_node");
const endpoint = envOr("BROKER_FRONTEND", "tcp://broker:5555");
const ownChannel = envOr("BOT_CHANNEL", `canal-${name}`);

// REQ com timeout: se o servidor não responder, o socket REQ fica travado,
// então ele é recriado (padrão "lazy pirate").
class Rpc {
  private sock!: Request;

  constructor(private endpoint: string, private who: string) {
    this.reset();
  }

  private reset(): void {
    this.sock?.close();
    this.sock = new Request({ linger: 0, receiveTimeout: 5000 });
    this.sock.connect(this.endpoint);
  }

  async call(req: Envelope): Promise<Envelope | null> {
    logMsg(this.who, "ENVIADO", req);
    try {
      await this.sock.send(encode(req));
      const [raw] = await this.sock.receive();
      const rep = decode(raw);
      logMsg(this.who, "RECEBIDO", rep);
      return rep;
    } catch (e) {
      console.error(`${this.who} | sem resposta válida, recriando socket (${(e as Error).message})`);
      this.reset();
      return null;
    }
  }
}

const rpc = new Rpc(endpoint, name);

// 1) Login (repetido até dar certo)
while (true) {
  const rep = await rpc.call(makeEnvelope(name, { loginRequest: { username: name } }));
  if (rep?.body === "loginResponse" && rep.loginResponse.success) {
    console.log(`${name} | login realizado com sucesso`);
    break;
  }
  console.log(`${name} | login falhou, tentando novamente em 2s`);
  await sleep(2000);
}

// 2) Lista canais e cria os que faltam
const wanted = ["geral", ownChannel];
while (true) {
  const rep = await rpc.call(makeEnvelope(name, { listChannelsRequest: {} }));
  if (rep?.body === "listChannelsResponse") {
    const existing: string[] = rep.listChannelsResponse.channels;
    for (const ch of wanted) {
      if (existing.includes(ch)) continue;
      await rpc.call(makeEnvelope(name, { createChannelRequest: { channelName: ch } }));
    }
  }
  await sleep(5000);
}