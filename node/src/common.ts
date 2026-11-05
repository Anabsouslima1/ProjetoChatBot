// Utilitários compartilhados por servidor e cliente (Node/TypeScript).
// Usa o MESMO proto/chat.proto das outras linguagens.
import protobuf from "protobufjs";

const PROTO_PATH = process.env.PROTO_PATH ?? "../proto/chat.proto";
const root = protobuf.loadSync(PROTO_PATH);
const EnvelopeType = root.lookupType("chat.Envelope");

export const MAX_MESSAGE_LEN = 1000; // tamanho máximo (bytes) do texto de uma publicação

// protobufjs converte snake_case -> camelCase (timestamp_ms -> timestampMs).
// `body` indica qual campo do oneof está preenchido (ex.: "loginRequest"),
// mas só existe em mensagens decodificadas.
export interface Envelope {
  timestampMs: number;
  sender: string;
  body?: string;
  [campo: string]: any;
}

export function nowMs(): number {
  return Date.now();
}

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

export function makeEnvelope(sender: string, body: Record<string, unknown> = {}): Envelope {
  return { timestampMs: nowMs(), sender, ...body };
}

export function encode(env: Envelope): Uint8Array {
  return EnvelopeType.encode(EnvelopeType.fromObject(env)).finish();
}

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
    default:
      return "(mensagem vazia/desconhecida)";
  }
}

export function logMsg(who: string, dir: string, e: Envelope): void {
  console.log(`${fmtTs(e.timestampMs)} | ${who} | ${dir} | de=${e.sender} | ${describe(e)}`);
}