// Cliente (bot). Ao conectar: login -> loop infinito de rodadas. Em cada rodada o bot:
//   1. lista os canais; se houver menos de 5, cria um novo;
//   2. se estiver inscrito em menos de 3 canais, se inscreve em mais um (escolhido ao acaso);
//   3. escolhe um canal da lista e publica 10 mensagens aleatórias com 1 s de intervalo.
// Em paralelo, recebe (SUB) e exibe tudo que chega nos canais em que o bot está inscrito.
// Parte 3: relógio lógico (Lamport): incrementa antes de enviar e usa max(local, recebido) ao receber.
import { Request, Subscriber } from "zeromq";
import { setTimeout as sleep } from "node:timers/promises";
import {
  decode,
  encode,
  envInt,
  envOr,
  fmtTs,
  logMsg,
  logicalClock,
  makeEnvelope,
  nowMs,
  stamp,
} from "./common.js";
import type { Envelope } from "./common.js";

const name = envOr("BOT_NAME", "bot_node");
const endpoint = envOr("BROKER_FRONTEND", "tcp://broker:5555");
const pubsub = envOr("PROXY_XPUB", "tcp://proxy:5558");
// Valores padrão do enunciado: 10 mensagens, 1 s de intervalo (ajustáveis só para testes rápidos)
const msgsPerRound = envInt("MSGS_PER_ROUND", 10);
const msgIntervalMs = envInt("MSG_INTERVAL_MS", 1000);

const pick = <T>(arr: T[]): T => arr[Math.floor(Math.random() * arr.length)];

// REQ com timeout: se o servidor não responder, o socket REQ fica travado,
// então ele é recriado (padrão "lazy pirate").
class Rpc {
  private sock!: Request;

  constructor(private endpoint: string, private who: string) {
    this.reset();
  }

  private reset(): void {
    this.sock?.close();
    this.sock = new Request({ linger: 0, receiveTimeout: 5000, sendTimeout: 5000 });
    this.sock.connect(this.endpoint);
  }

  async call(req: Envelope): Promise<Envelope | null> {
    stamp(req); // hora + relógio lógico incrementado, imediatamente antes de enviar
    logMsg(this.who, "ENVIADO", req);
    try {
      await this.sock.send(encode(req));
      const [raw] = await this.sock.receive();
      const rep = decode(raw);
      logicalClock.observe(rep.logicalClock); // relógio lógico: max(local, recebido)
      logMsg(this.who, "RECEBIDO", rep);
      return rep;
    } catch (e) {
      console.error(`${this.who} | sem resposta válida, recriando socket (${(e as Error).message})`);
      this.reset();
      return null;
    }
  }
}

// Assinante (SUB): um único socket pode assinar vários tópicos (canais).
class Subscriptions {
  private sub = new Subscriber();
  private channels = new Set<string>();

  constructor(endpoint: string, private who: string) {
    this.sub.connect(endpoint);
  }

  count(): number {
    return this.channels.size;
  }

  has(channel: string): boolean {
    return this.channels.has(channel);
  }

  subscribe(channel: string): void {
    if (this.channels.has(channel)) return;
    this.channels.add(channel);
    this.sub.subscribe(channel);
  }

  // Loop de recepção: roda em paralelo com o resto do bot.
  async listen(): Promise<void> {
    // Mensagem do Pub/Sub: frame 1 = tópico (canal), frame 2 = Envelope
    for await (const [topic, body] of this.sub) {
      const receivedAt = nowMs();
      const channel = topic.toString();
      // A inscrição do ZeroMQ casa por prefixo ("geral" também recebe "geral2"):
      // só exibimos canais em que o bot realmente se inscreveu.
      if (!this.channels.has(channel)) continue;
      try {
        const e = decode(body);
        if (e.body !== "publication") continue;
        // Relógio lógico: contador = max(contador, valor da mensagem). Retorna o valor já atualizado.
        const localClock = logicalClock.observe(e.logicalClock);
        console.log(
          `${fmtTs(receivedAt)} | ${this.who} | MENSAGEM | canal=${channel}` +
            ` | relogio_msg=${e.logicalClock} | relogio_local=${localClock}` +
            ` | de=${e.publication.author} | enviada_em=${fmtTs(e.timestampMs)}` +
            ` | recebida_em=${fmtTs(receivedAt)} | "${e.publication.message}"`,
        );
      } catch {
        // mensagem inválida: ignora
      }
    }
  }
}

const rpc = new Rpc(endpoint, name);
const subs = new Subscriptions(pubsub, name);
subs.listen().catch((e) => console.error(`${name} | erro no SUB:`, e));

// Login (repetido até dar certo)
while (true) {
  const rep = await rpc.call(makeEnvelope(name, { loginRequest: { username: name } }));
  if (rep?.body === "loginResponse" && rep.loginResponse.success) {
    console.log(`${name} | login realizado com sucesso`);
    break;
  }
  console.log(`${name} | login falhou, tentando novamente em 2s`);
  await sleep(2000);
}

const phrases = [
  "Olá, pessoal!",
  "How are you?",
  "Bonjour, ça va?",
  "¡Buenos días!",
  "Mir geht's gut!",
  "Ciao, come stai?",
  "Kon'nichiwa!",
  "Annyeong! Jal jinaeyo?",
  "Nǐ hǎo, péngyou!",
  "Salve! Quid agis?",
  "Geia sou! Ti kaneis?",
  "Privet! Kak dela?",
  "Hej! Hur mår du?"
];

while (true) {
  // Lista os canais disponíveis
  const lrep = await rpc.call(makeEnvelope(name, { listChannelsRequest: {} }));
  if (lrep?.body !== "listChannelsResponse") {
    await sleep(2000);
    continue;
  }
  const channels: string[] = [...lrep.listChannelsResponse.channels];

  // 1) menos de 5 canais -> cria um novo
  if (channels.length < 5) {
    const base = `canal-${name}`.slice(0, 27);
    const novo = `${base}-${1000 + Math.floor(Math.random() * 9000)}`;
    const crep = await rpc.call(makeEnvelope(name, { createChannelRequest: { channelName: novo } }));
    if (crep?.body === "createChannelResponse" && crep.createChannelResponse.success) {
      channels.push(novo);
    }
  }

  // 2) inscrito em menos de 3 canais -> se inscreve em mais um (ao acaso)
  if (subs.count() < 3) {
    const candidates = channels.filter((c) => !subs.has(c));
    if (candidates.length > 0) {
      const c = pick(candidates);
      subs.subscribe(c);
      console.log(`${name} | inscrito no canal ${c} (total de inscrições: ${subs.count()})`);
      // A inscrição leva um instante para chegar aos publicadores (ZeroMQ "slow joiner");
      // esperamos um pouco para não perder as primeiras mensagens do canal.
      await sleep(300);
    }
  }

  // 3) escolhe um canal e publica 10 mensagens, 1 por segundo
  if (channels.length === 0) {
    await sleep(1000);
    continue;
  }
  const target = pick(channels);
  for (let i = 1; i <= msgsPerRound; i++) {
    await rpc.call(
      makeEnvelope(name, {
        publishRequest: { channel: target, message: `${pick(phrases)} (#${i} de ${name})` },
      }),
    );
    await sleep(msgIntervalMs);
  }
}
