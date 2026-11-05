// Broker: faz a ponte entre os clientes (REQ) e os servidores (REP).
//   clientes   -> tcp://*:5555 (ROUTER)
//   servidores -> tcp://*:5556 (DEALER)
// O DEALER distribui as requisições entre os servidores conectados (round-robin),
// o que permite adicionar/remover servidores sem parar o serviço.
import { Router, Dealer } from "zeromq";
import { decode, describe, envOr } from "./common.js";

const feAddr = envOr("BROKER_FRONTEND_BIND", "tcp://*:5555");
const beAddr = envOr("BROKER_BACKEND_BIND", "tcp://*:5556");

const frontend = new Router();
const backend = new Dealer();
await frontend.bind(feAddr);
await backend.bind(beAddr);

console.log(`[broker] frontend (clientes) em ${feAddr} | backend (servidores) em ${beAddr}`);

// Frames: [identidade do cliente, vazio, payload]. O payload é sempre o último.
// O broker só repassa os frames sem alterar; decodificar é apenas para o log.
function logFrames(dir: string, frames: Uint8Array[]): void {
  try {
    const env = decode(frames[frames.length - 1]);
    const ts = new Date(env.timestampMs).toISOString();
    console.log(`${ts} | broker | ${dir} | de=${env.sender} | ${describe(env)}`);
  } catch {
    console.log(`[broker] ${dir} | (mensagem não decodificável)`);
  }
}

let clientCount = 0;
let serverCount = 0;

async function clientsToServers(): Promise<void> {
  for await (const frames of frontend) {
    clientCount++;
    logFrames(`cliente->servidor #${clientCount}`, frames);
    await backend.send(frames);
  }
}

async function serversToClients(): Promise<void> {
  for await (const frames of backend) {
    serverCount++;
    logFrames(`servidor->cliente #${serverCount}`, frames);
    await frontend.send(frames);
  }
}

await Promise.all([clientsToServers(), serversToClients()]);