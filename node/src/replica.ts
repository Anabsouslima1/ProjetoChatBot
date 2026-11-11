// Réplica dos dados (Parte 5): consistência eventual, multi-mestre.
//
// Todo dado aceito por ESTE servidor (login, canal, publicação) vira um ReplicaRecord com id único
// (origin, seq). A "origem" é <servidor>@<instante de partida>, para que um servidor que reinicia
// sem disco nunca reaproveite ids antigos. records.bin guarda TODOS os registros (os próprios e os
// recebidos de outros servidores); logins.tsv, channels.txt e publications.bin são visões derivadas.
// Aplicar o mesmo registro duas vezes não faz nada (idempotente).
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { clockMs, decodeRecord, encode, encodeRecord, logicalClock, nowMs } from "./common.js";
import type { Envelope, ReplicaRecord } from "./common.js";

export interface ReplicaStats {
  total: number;
  logins: number;
  channels: number;
  publications: number;
  hash: string; // resumo dos ids: servidores com o mesmo conjunto de registros têm o mesmo hash
}

type NewRecord = Omit<ReplicaRecord, "origin" | "seq">;
interface Progress {
  contig: number; // tenho 1..contig de uma origem
  above: Set<number>; // seqs recebidas além de contig+1 (chegaram fora de ordem)
}

function appendFramed(path: string, payload: Uint8Array): boolean {
  try {
    const hdr = Buffer.alloc(4);
    hdr.writeUInt32BE(payload.length);
    appendFileSync(path, Buffer.concat([hdr, payload]));
    return true;
  } catch {
    return false;
  }
}

export class Replica {
  readonly origin: string;
  private all = new Map<string, Map<number, ReplicaRecord>>();
  private progress = new Map<string, Progress>();
  private channelSet = new Set<string>();
  private outbox: ReplicaRecord[] = [];
  private wake: (() => void) | null = null;
  private nextSeq = 0;
  private total = 0;
  private logins = 0;
  private channelRecords = 0;
  private publications = 0;

  constructor(private dir: string, serverName: string) {
    mkdirSync(dir, { recursive: true });
    this.origin = `${serverName}@${nowMs()}`;
    this.load();
  }

  // ---- escritas feitas por este servidor (a pedido de um cliente) ----
  addLogin(user: string, ts: number): boolean {
    return this.addLocal({
      kind: "KIND_LOGIN",
      name: user,
      text: "",
      author: "",
      timestampMs: ts,
      logicalClock: logicalClock.current(),
    });
  }

  // 0 = criado, 1 = já existe (em qualquer servidor), 2 = erro de disco
  addChannel(channel: string): 0 | 1 | 2 {
    if (this.channelSet.has(channel)) return 1;
    const ok = this.addLocal({
      kind: "KIND_CHANNEL",
      name: channel,
      text: "",
      author: "",
      timestampMs: clockMs(),
      logicalClock: logicalClock.current(),
    });
    return ok ? 0 : 2;
  }

  // pubmsg = a publicação já carimbada (hora e relógio lógico) que vai para o tópico
  addPublication(pubmsg: Envelope): boolean {
    return this.addLocal({
      kind: "KIND_PUBLICATION",
      name: pubmsg.publication.channel,
      text: pubmsg.publication.message,
      author: pubmsg.publication.author,
      timestampMs: pubmsg.timestampMs,
      logicalClock: pubmsg.logicalClock,
    });
  }

  channels(): string[] {
    return [...this.channelSet].sort();
  }

  // ---- registros vindos de outros servidores. Retorna quantos eram novos ----
  applyRemote(recs: ReplicaRecord[]): number {
    let added = 0;
    for (const r of recs) {
      if (!r.origin || r.seq <= 0 || this.known(r)) continue;
      this.store(r, true);
      added++;
    }
    return added;
  }

  // ---- anti-entropia ----
  // até que seq (sem buracos) temos registros de cada origem
  version(): { origin: string; lastSeq: number }[] {
    return [...this.progress].map(([origin, p]) => ({ origin, lastSeq: p.contig }));
  }

  // registros que quem pediu (com a versão "have") ainda não tem; no máximo max
  missing(have: Map<string, number>, max: number): { records: ReplicaRecord[]; more: boolean } {
    const out: ReplicaRecord[] = [];
    for (const origin of [...this.all.keys()].sort()) {
      const recs = this.all.get(origin)!;
      const from = have.get(origin) ?? 0;
      const seqs = [...recs.keys()].filter((s) => s > from).sort((a, b) => a - b);
      for (const s of seqs) {
        if (out.length >= max) return { records: out, more: true };
        out.push(recs.get(s)!);
      }
    }
    return { records: out, more: false };
  }

  // ---- fila de propagação (usada pela tarefa que empurra registros aos outros servidores) ----
  async waitOutbox(timeoutMs: number, max: number): Promise<ReplicaRecord[]> {
    if (this.outbox.length === 0) {
      await new Promise<void>((resolve) => {
        const t = setTimeout(() => {
          this.wake = null;
          resolve();
        }, timeoutMs);
        this.wake = () => {
          clearTimeout(t);
          resolve();
        };
      });
    }
    return this.outbox.splice(0, max);
  }

  stats(): ReplicaStats {
    // FNV-1a (32 bits) sobre "origem#seq;" de todos os registros, por origem e depois por seq.
    // Mesmo algoritmo do servidor C++, então o hash é comparável entre as linguagens.
    let h = 2166136261;
    for (const origin of [...this.all.keys()].sort()) {
      const recs = this.all.get(origin)!;
      for (const seq of [...recs.keys()].sort((a, b) => a - b)) {
        for (const c of Buffer.from(`${origin}#${seq};`, "utf8")) h = Math.imul(h ^ c, 16777619) >>> 0;
      }
    }
    return {
      total: this.total,
      logins: this.logins,
      channels: this.channelRecords,
      publications: this.publications,
      hash: h.toString(16).padStart(8, "0"),
    };
  }

  // ---------------------------------------------------------------------------

  private known(r: ReplicaRecord): boolean {
    return this.all.get(r.origin)?.has(r.seq) ?? false;
  }

  private addLocal(partial: NewRecord): boolean {
    const r: ReplicaRecord = { ...partial, origin: this.origin, seq: ++this.nextSeq };
    if (!this.store(r, true)) return false; // falha de disco: o cliente recebe ERROR_INTERNAL
    this.outbox.push(r);
    const w = this.wake;
    this.wake = null;
    w?.();
    return true;
  }

  // Guarda em memória e em disco (records.bin + visões). Retorna false se o disco falhou.
  private store(r: ReplicaRecord, writeViews: boolean): boolean {
    let ok = appendFramed(join(this.dir, "records.bin"), encodeRecord(r));
    this.index(r);
    if (writeViews) ok = this.writeView(r) && ok;
    return ok;
  }

  private index(r: ReplicaRecord): void {
    let recs = this.all.get(r.origin);
    if (!recs) this.all.set(r.origin, (recs = new Map()));
    recs.set(r.seq, r);

    let p = this.progress.get(r.origin);
    if (!p) this.progress.set(r.origin, (p = { contig: 0, above: new Set() }));
    if (r.seq === p.contig + 1) {
      p.contig++;
      while (p.above.delete(p.contig + 1)) p.contig++;
    } else if (r.seq > p.contig + 1) {
      p.above.add(r.seq);
    }

    this.total++;
    switch (r.kind) {
      case "KIND_LOGIN":
        this.logins++;
        break;
      case "KIND_CHANNEL":
        this.channelRecords++;
        this.channelSet.add(r.name);
        break;
      case "KIND_PUBLICATION":
        this.publications++;
        break;
    }
  }

  // Visões legíveis (as mesmas dos requisitos das partes 1 e 2)
  private writeView(r: ReplicaRecord): boolean {
    try {
      switch (r.kind) {
        case "KIND_LOGIN":
          appendFileSync(join(this.dir, "logins.tsv"), `${r.timestampMs}\t${r.name}\n`);
          return true;
        case "KIND_CHANNEL":
          appendFileSync(join(this.dir, "channels.txt"), `${r.name}\n`);
          return true;
        case "KIND_PUBLICATION": {
          const at = r.origin.indexOf("@");
          const e: Envelope = {
            timestampMs: r.timestampMs,
            sender: at < 0 ? r.origin : r.origin.slice(0, at),
            logicalClock: r.logicalClock,
            publication: { channel: r.name, message: r.text, author: r.author },
          };
          return appendFramed(join(this.dir, "publications.bin"), encode(e));
        }
        default:
          return true;
      }
    } catch {
      return false;
    }
  }

  private load(): void {
    const file = join(this.dir, "records.bin");
    if (!existsSync(file)) return;
    const buf = readFileSync(file);
    let off = 0;
    while (off + 4 <= buf.length) {
      const n = buf.readUInt32BE(off);
      if (off + 4 + n > buf.length) break; // registro truncado no fim: ignora
      try {
        const r = decodeRecord(buf.subarray(off + 4, off + 4 + n));
        if (!this.known(r)) this.index(r);
      } catch {
        // registro inválido: ignora
      }
      off += 4 + n;
    }
  }
}