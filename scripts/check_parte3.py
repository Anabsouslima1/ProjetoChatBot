#!/usr/bin/env python3
"""Confere os requisitos da parte 3 (relógios e heartbeat) a partir do log do `docker compose`.

Uso:
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml up -d --build
    sleep 90
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml logs --no-color > run.log
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml down
    python3 scripts/check_parte3.py run.log

Formato esperado das linhas de mensagem (igual em todas as linguagens):
    <hora> | <quem> | ENVIADO/RECEBIDO/PUBLICADO | de=<remetente> | relogio=<N> | <conteúdo>
O que não dá para saber pelo log aparece como [ ? ]. A remoção de servidor parado deve ser testada à mão.
"""
import re
import sys
from collections import defaultdict

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LINE = re.compile(r"^(?P<svc>[\w.]+?)-\d+\s+\|\s?(?P<msg>.*)$")
MSG = re.compile(r"\|\s*(ENVIADO|RECEBIDO|PUBLICADO)\s*\|")
CLOCK = re.compile(r"relogio=(\d+)")
FROM = re.compile(r"de=([\w-]+)")


def main(path):
    lines = defaultdict(list)
    for raw in open(path, encoding="utf-8", errors="replace"):
        m = LINE.match(ANSI.sub("", raw.rstrip("\n")))
        if m:
            lines[m.group("svc")].append(m.group("msg"))

    results = []

    def add(status, title, detail=""):
        results.append((status, title, detail))

    procs = sorted(s for s in lines if s.startswith(("server_", "client_", "reference")))
    no_clock, not_increasing, lamport_bad, max_clock = {}, {}, {}, {}
    for s in procs:
        last_sent, max_recv = 0, 0
        n_msgs = n_noclock = n_noninc = n_lamport = 0
        for l in lines[s]:
            m = MSG.search(l)
            if not m:
                continue
            n_msgs += 1
            c = CLOCK.search(l)
            if not c:
                n_noclock += 1
                continue
            v = int(c.group(1))
            if m.group(1) == "RECEBIDO":
                max_recv = max(max_recv, v)
            else:  # ENVIADO / PUBLICADO: contador incrementado antes de enviar
                if v <= last_sent:
                    n_noninc += 1
                if v <= max_recv:
                    n_lamport += 1
                last_sent = max(last_sent, v)
            max_clock[s] = max(max_clock.get(s, 0), v)
        if n_msgs:
            no_clock[s], not_increasing[s], lamport_bad[s] = n_noclock, n_noninc, n_lamport

    add("OK" if procs else "FALHA", "Processos encontrados no log", ", ".join(procs) or "nenhum")

    bad = {s: n for s, n in no_clock.items() if n}
    add("OK" if not bad else "FALHA", "Toda mensagem enviada/recebida mostra relogio=N",
        "; ".join(f"{s}: {n} sem relogio" for s, n in bad.items()))

    bad = {s: n for s, n in not_increasing.items() if n}
    add("OK" if not bad else "FALHA", "Relógio dos envios sempre cresce (incrementa antes de enviar)",
        "; ".join(f"{s}: {n} envio(s) que não aumentou" for s, n in bad.items()))

    bad = {s: n for s, n in lamport_bad.items() if n}
    add("OK" if not bad else "FALHA", "Regra de Lamport: todo envio é maior que qualquer relógio já recebido",
        "; ".join(f"{s}: {n} violação(ões)" for s, n in bad.items()))

    # Servidores: rank e sincronização
    servers = [s for s in procs if s.startswith("server_")]
    got_rank = [s for s in servers if any(re.search(r"rank", l, re.I) and re.search(r"\d", l) and
                                          ("rank_response" in l or "rank recebido" in l.lower() or
                                           "rank:" in l.lower() or "rank=" in l.lower()) for l in lines[s])]
    bad = [s for s in servers if s not in got_rank]
    add("OK" if not bad else "FALHA", "Servidores receberam o rank da referência", f"sem rank: {', '.join(bad)}" if bad else "")

    synced = [s for s in servers if any(re.search(r"sincroniz", l, re.I) for l in lines[s])]
    bad = [s for s in servers if s not in synced]
    add("OK" if not bad else "FALHA", "Servidores sincronizaram o relógio com a referência", f"sem sincronização: {', '.join(bad)}" if bad else "")

    # Heartbeat a cada 10 mensagens de clientes
    for s in servers:
        between, counts, started = 0, [], False
        for l in lines[s]:
            if MSG.search(l):
                m = MSG.search(l)
                if m.group(1) == "RECEBIDO" and (FROM.search(l) and FROM.search(l).group(1).startswith("bot_")):
                    between += 1
                elif m.group(1) == "ENVIADO" and "heartbeat_request" in l:
                    counts.append(between)
                    between = 0
        if not counts:
            add("FALHA", f"{s}: enviou heartbeats", "nenhum heartbeat_request no log")
            continue
        off = [c for c in counts[1:] if c != 10]  # o 1º pode ser parcial se o servidor entrou no meio
        add("OK" if not off else "FALHA", f"{s}: heartbeat a cada 10 mensagens de clientes",
            f"{len(counts)} heartbeat(s); mensagens entre eles: {counts[:12]}")

    # Referência
    ref = lines.get("reference", [])
    if ref:
        hb = sum(1 for l in ref if "heartbeat_request" in l and "RECEBIDO" in l)
        add("OK" if hb else "FALHA", "Referência recebeu heartbeats", f"{hb} heartbeat(s)")
    else:
        add("?", "Referência", "sem linhas de log do serviço 'reference'")

    add("?", "Remoção de servidor parado", "teste à mão: docker compose stop <servidor>; conferir que some da lista")

    sym = {"OK": "[ OK ]", "FALHA": "[FALHA]", "?": "[ ?  ]"}
    print("Verificação da parte 3 —", path, "\n")
    for st, t, d in results:
        print(f"{sym[st]} {t}" + (f"\n         {d}" if d else ""))
    print("\nMaior relógio lógico por processo:", ", ".join(f"{s}={v}" for s, v in sorted(max_clock.items())))
    sys.exit(1 if any(r[0] == "FALHA" for r in results) else 0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
