// Utilitários compartilhados por servidor e cliente (Node/TypeScript).
// Usa o MESMO proto/chat.proto das outras linguagens.
import protobuf from "protobufjs";

const PROTO_PATH = process.env.PROTO_PATH ?? "../proto/chat.proto";
const root = protobuf.loadSync(PROTO_PATH);
const EnvelopeType = root.lookupType("chat.Envelope");

export const MAX_MESSAGE_LEN = 1000; // tamanho máximo (bytes) do texto de uma publicação

// protobufjs converte snake_case -> camelCase (timestamp_ms -> timestampMs, logical_clock -> logicalClock).
// `body` indica qual campo do oneof está preenchido (ex.: "loginRequest"),
// mas só existe em mensagens decodificadas.
export interface Envelope {
  timestampMs: number;
  sender: string;
  logicalClock: number;
  body?: string;
  [campo: string]: any;
}

// ---------------------------------------------------------------------------
// Relógio físico
//   nowMs()   : hora da máquina, sem correção (usada para medir a sincronização)
//   clockMs() : hora usada nas mensagens = hora da máquina + deslocamento calculado pela
//               sincronização com a referência (só os servidores alteram o deslocamento).
//               Nunca retorna um valor menor que o anterior, mesmo se o deslocamento diminuir.
// ---------------------------------------------------------------------------
export function nowMs(): number {
  return Date.now();
}

let clockOffsetMs = 0;
let lastClockMs = 0;

export function getClockOffset(): number {
  return clockOffsetMs;
}

export function setClockOffset(ms: number): void {
  clockOffsetMs = ms;
}

export function clockMs(): number {
  let t = nowMs() + clockOffsetMs;
  if (t < lastClockMs) t = lastClockMs;
  lastClockMs = t;
  return t;
}

// ---------------------------------------------------------------------------
// Relógio lógico (Lamport) - um contador por processo, iniciado junto com ele.
//   tick()    : incrementa ANTES de enviar uma mensagem; o valor vai na mensagem.
//   observe() : ao RECEBER, contador = max(contador, valor recebido). Retorna o novo valor.
// ---------------------------------------------------------------------------
class LogicalClock {
  private value = 0;

  tick(): number {
    return ++this.value;
  }

  observe(received: number): number {
    if (received > this.value) this.value = received;
    return this.value;
  }

  current(): number {
    return this.value;
  }
}

export const logicalClock = new LogicalClock();

// Carimba a mensagem imediatamente antes do envio: hora (corrigida) + relógio lógico incrementado.
export function stamp(e: Envelope): void {
  e.timestampMs = clockMs();
  e.logicalClock = logicalClock.tick();
}

// ---------------------------------------------------------------------------

export function fmtTs(ms: number): string {
  return new Date(ms).toISOString();
}

export function envOr(key: string, fallback: string): string {
  const v = process.env[key];
  return v && v.length > 0 ? v : fallback;
}

export function envInt(key: string, fallback: number): number {
  const n = parseInt(envOr(key, String(fallback)), 10);
  return Number.isNaN(n) ? fallback : n;
}

// Cria uma mensagem com o remetente. A hora e o relógio lógico são preenchidos por stamp(),
// logo antes de enviar.
export function makeEnvelope(sender: string, body: Record<string, unknown> = {}): Envelope {
  return { timestampMs: clockMs(), sender, logicalClock: 0, ...body };
}

// Envia a mensagem como está. Chame stamp(e) antes, logo antes do envio.
export function encode(env: Envelope): Uint8Array {
  return EnvelopeType.encode(EnvelopeType.fromObject(env)).finish();
}

// Só decodifica. Ao RECEBER uma mensagem, quem chama também deve fazer
// logicalClock.observe(env.logicalClock) (o broker e o --dump não atualizam o relógio).
export function decode(buf: Uint8Array): Envelope {
  const msg = EnvelopeType.decode(buf);
  return EnvelopeType.toObject(msg, {
    longs: Number,
    enums: String,
    oneofs: true,
    defaults: true,
  }) as Envelope;
}

// Nome do usuário/canal: 1..32 caracteres entre [A-Za-z0-9_-]
export function validName(s: string): boolean {
  return /^[A-Za-z0-9_-]{1,32}$/.test(s);
}

// Texto de uma publicação: 1..MAX_MESSAGE_LEN bytes
export function validMessage(s: string): boolean {
  const n = Buffer.byteLength(s, "utf8");
  return n >= 1 && n <= MAX_MESSAGE_LEN;
}

function status(r: Record<string, any>): string {
  let o = `success=${r.success}`;
  if (!r.success) o += `, erro=${r.error} (${r.errorMessage})`;
  return o;
}

const BODIES = [
  "loginRequest",
  "loginResponse",
  "createChannelRequest",
  "createChannelResponse",
  "listChannelsRequest",
  "listChannelsResponse",
  "publishRequest",
  "publishResponse",
  "publication",
  "rankRequest",
  "rankResponse",
  "listServersRequest",
  "listServersResponse",
  "heartbeatRequest",
  "heartbeatResponse",
];

// Texto legível do conteúdo da mensagem (para os logs de envio/recebimento)
export function describe(e: Envelope): string {
  // `body` só existe em mensagens decodificadas; nas que montamos à mão,
  // descobrimos o tipo pelo campo presente.
  const body = e.body ?? BODIES.find((k) => e[k] !== undefined);
  switch (body) {
    case "loginRequest":
      return `login_request{username=${e.loginRequest.username}}`;
    case "loginResponse":
      return `login_response{${status(e.loginResponse)}}`;
    case "createChannelRequest":
      return `create_channel_request{channel_name=${e.createChannelRequest.channelName}}`;
    case "createChannelResponse":
      return `create_channel_response{${status(e.createChannelResponse)}}`;
    case "listChannelsRequest":
      return "list_channels_request{}";
    case "listChannelsResponse":
      return `list_channels_response{channels=[${e.listChannelsResponse.channels.join(", ")}]}`;
    case "publishRequest":
      return `publish_request{channel=${e.publishRequest.channel}, message="${e.publishRequest.message}"}`;
    case "publishResponse":
      return `publish_response{${status(e.publishResponse)}}`;
    case "publication":
      return `publication{channel=${e.publication.channel}, author=${e.publication.author}, message="${e.publication.message}"}`;
    case "rankRequest":
      return `rank_request{name=${e.rankRequest.name}}`;
    case "rankResponse":
      return `rank_response{rank=${e.rankResponse.rank}}`;
    case "listServersRequest":
      return "list_servers_request{}";
    case "listServersResponse":
      return `list_servers_response{servers=[${(e.listServersResponse.servers as any[])
        .map((s) => `${s.name}:${s.rank}`)
        .join(", ")}]}`;
    case "heartbeatRequest":
      return `heartbeat_request{name=${e.heartbeatRequest.name}}`;
    case "heartbeatResponse":
      return `heartbeat_response{success=${e.heartbeatResponse.success}, time_ms=${e.heartbeatResponse.timeMs}}`;
    default:
      return "(mensagem vazia/desconhecida)";
  }
}

// timestamp | quem | ENVIADO/RECEBIDO | de=... | relogio=<relógio lógico da mensagem> | conteúdo
export function logMsg(who: string, dir: string, e: Envelope): void {
  console.log(
    `${fmtTs(e.timestampMs)} | ${who} | ${dir} | de=${e.sender} | relogio=${e.logicalClock} | ${describe(e)}`,
  );
}
