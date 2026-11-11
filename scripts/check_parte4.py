#!/usr/bin/env python3
"""Confere os requisitos da parte 4 (coordenador, eleição, sincronização) a partir do log do compose.

Uso:
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml up -d --build
    sleep 90
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml logs --no-color > run.log
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml down
    python3 scripts/check_parte4.py run.log

Formato esperado das mensagens (todas as linguagens):
    <hora> | <quem> | ENVIADO/RECEBIDO/PUBLICADO | de=<remetente> | relogio=<N> | <conteúdo>
com o conteúdo usando os nomes do proto: clock_request{}, clock_response{time_ms=..},
election_request{..}, election_response{ok=..}, coordinator_announcement{name=..},
heartbeat_response{success=.., time_ms=..}, rank_response{rank=..}.
Teste manual (o log normal não cobre): pare o coordenador (docker compose stop <servidor>) e confira
que os outros elegem um novo e passam a sincronizar com ele; rode este script de novo nesse log.
"""
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LINE = re.compile(r"^(?P<svc>[\w.]+?)-\d+\s+\|\s?(?P<msg>.*)$")
MSG = re.compile(r"^\s*(?P<ts>\d{4}-\d\d-\d\d[ T][\d:.]+Z?)\s*\|.*?\|\s*(?P<dir>ENVIADO|RECEBIDO|PUBLICADO)\s*\|")
CLOCK = re.compile(r"relogio=(\d+)")
FROM = re.compile(r"de=([\w-]+)")
ANN = re.compile(r"coordinator_announcement\{name=([\w-]+)")
RANK = re.compile(r"rank_response\{rank=(\d+)")
HBT = re.compile(r"heartbeat_response\{success=true,\s*time_ms=(-?\d+)")
OFFSET = re.compile(r"deslocamento\s+(-?\d+)\s*ms\s*->\s*(-?\d+)\s*ms")


def to_s(ts):
    return datetime.fromisoformat(ts.replace(" ", "T").rstrip("Z")).replace(tzinfo=timezone.utc).timestamp()


def main(path):
    lines = defaultdict(list)
    for raw in open(path, encoding="utf-8", errors="replace"):
        m = LINE.match(ANSI.sub("", raw.rstrip("\n")))
        if m:
            lines[m.group("svc")].append(m.group("msg"))

    results = []

    def add(status, title, detail=""):
        results.append((status, title, detail))

    servers = sorted(s for s in lines if s.startswith("server_"))
    procs = sorted(s for s in lines if s.startswith(("server_", "client_", "reference")))

    # --- relógio lógico (continua valendo) ---
    bad_clock, bad_inc, bad_lamport = {}, {}, {}
    examples = defaultdict(list)  # (processo, tipo) -> linhas que violaram (até 2)
    for s in procs:
        last_sent = max_recv = 0
        max_recv_line = ""
        nc = ni = nl = 0
        for l in lines[s]:
            m = MSG.match(l)
            if not m:
                continue
            c = CLOCK.search(l)
            if not c:
                nc += 1
                continue
            v = int(c.group(1))
            if m.group("dir") == "RECEBIDO":
                if v > max_recv:
                    max_recv, max_recv_line = v, l
            else:
                if v <= last_sent and len(examples[(s, "inc")]) < 2:
                    examples[(s, "inc")].append(l)
                if v <= max_recv and len(examples[(s, "lamport")]) < 2:
                    examples[(s, "lamport")].append(f"{l}\n             (maior RECEBIDO antes dele: {max_recv_line})")
                ni += v <= last_sent
                nl += v <= max_recv
                last_sent = max(last_sent, v)
        bad_clock[s], bad_inc[s], bad_lamport[s] = nc, ni, nl
    for title, d in (("Toda mensagem mostra relogio=N", bad_clock),
                     ("Relógio dos envios sempre cresce", bad_inc),
                     ("Regra de Lamport: todo envio é maior que qualquer relógio recebido", bad_lamport)):
        bad = {s: n for s, n in d.items() if n}
        kind = "inc" if "cresce" in title else "lamport" if "Lamport" in title else None
        detail = "; ".join(f"{s}: {n}" for s, n in bad.items())
        if kind:
            for s in bad:
                for ex in examples[(s, kind)]:
                    detail += f"\n           {s}: {ex}"
        add("OK" if not bad else "FALHA", title, detail)

    # --- referência não devolve mais a hora ---
    times = [int(m.group(1)) for l in lines.get("reference", []) for m in [HBT.search(l)] if m]
    if not times:
        for s in servers:  # fallback: o que os servidores receberam
            times += [int(m.group(1)) for l in lines[s] for m in [HBT.search(l)] if m]
    if not times:
        add("?", "Heartbeat da referência sem hora (time_ms = 0)", "nenhuma heartbeat_response com success=true no log")
    else:
        nz = [t for t in times if t != 0]
        add("OK" if not nz else "FALHA", "Heartbeat da referência sem hora (time_ms = 0)",
            f"{len(times)} resposta(s)" if not nz else f"{len(nz)} de {len(times)} respostas ainda trazem a hora")

    # Servidor "parado": o último log dele termina bem antes do fim (ex.: docker compose stop no teste de queda)
    last_ts = {}
    for s in servers:
        stamps = [to_s(m.group("ts")) for l in lines[s] for m in [MSG.match(l)] if m]
        if stamps:
            last_ts[s] = max(stamps)
    end = max(last_ts.values()) if last_ts else 0
    stopped = {s for s, t in last_ts.items() if end - t > 8}
    if stopped:
        add("?", "Servidores parados durante o teste (ignorados nas conferências de acordo/rank)", ", ".join(sorted(stopped)))

    # --- anúncios de coordenador ---
    announcements = []  # (ts, publicador, nome)
    for s in servers:
        for l in lines[s]:
            m = MSG.match(l)
            a = ANN.search(l)
            if m and a and m.group("dir") == "PUBLICADO":
                announcements.append((m.group("ts"), s, a.group(1)))
    announcements.sort()
    if not announcements:
        add("FALHA", "Algum servidor foi eleito e anunciou no tópico servers", "nenhum coordinator_announcement PUBLICADO")
    else:
        names = []
        for _, _, n in announcements:
            if not names or names[-1] != n:
                names.append(n)
        add("OK", "Algum servidor foi eleito e anunciou no tópico servers",
            "coordenadores ao longo do log: " + " -> ".join(names))
    wrong_pub = [f"{s} anunciou {n}" for _, s, n in announcements if s != n]
    add("OK" if not wrong_pub else "FALHA", "Só o próprio eleito se anuncia", "; ".join(sorted(set(wrong_pub))[:5]))

    # --- participação ---
    def talks_part4(s):
        return any(re.search(r"clock_request|election_request|coordinator_announcement", l) for l in lines[s])
    participants = [s for s in servers if talks_part4(s)]
    silent = [s for s in servers if s not in participants and s not in stopped]
    add("OK" if not silent else "FALHA", "Todos os servidores participam (relógio/eleição/coordenador)",
        f"sem nenhuma mensagem da parte 4: {', '.join(silent)}" if silent else f"{len(participants)} servidores")

    # --- coordenador final igual para todos ---
    final = {}
    for s in [p for p in participants if p not in stopped]:
        last = None
        for l in lines[s]:
            m, a = MSG.match(l), ANN.search(l)
            if m and a:
                last = (m.group("ts"), a.group(1))
        if last:
            final[s] = last[1]
    # considera apenas anúncios (recebidos ou publicados) — o mais recente de cada servidor
    if final:
        distinct = set(final.values())
        add("OK" if len(distinct) == 1 else "FALHA", "Todos os servidores concordam sobre o coordenador (último anúncio visto)",
            ", ".join(f"{s}->{c}" for s, c in sorted(final.items())))
    else:
        add("FALHA", "Todos os servidores concordam sobre o coordenador", "nenhum anúncio visto pelos servidores")
    coordinator = announcements[-1][2] if announcements else None

    # --- bully: coordenador deve ter o maior rank ---
    ranks = {}
    for s in servers:
        for l in lines[s]:
            m = MSG.match(l)
            r = RANK.search(l)
            if m and r and m.group("dir") == "RECEBIDO":
                ranks[s] = int(r.group(1))
    if coordinator and len(ranks) >= 2:
        alive = {s: r for s, r in ranks.items() if s in participants and s not in stopped}
        top = max(alive, key=alive.get) if alive else None
        add("OK" if top == coordinator else "FALHA", "Bully: o coordenador é o servidor de maior rank",
            f"coordenador={coordinator}; maior rank={top}; ranks={dict(sorted(ranks.items()))}")
    else:
        add("?", "Bully: o coordenador é o servidor de maior rank", "ranks não encontrados no log dos servidores")

    # --- sincronização a cada 15 mensagens ---
    for s in participants:
        between, intervals, reqs, resps = 0, [], 0, 0
        for l in lines[s]:
            m = MSG.match(l)
            if not m:
                continue
            d = m.group("dir")
            if d == "RECEBIDO" and (FROM.search(l) or [None, ""])[1].startswith("bot_"):
                between += 1
            elif d == "ENVIADO" and "clock_request" in l:
                reqs += 1
                intervals.append(between)
                between = 0
            elif d == "RECEBIDO" and "clock_response" in l:
                resps += 1
        if not reqs:
            if any(p == s for _, p, _ in announcements):
                add("OK", f"{s}: foi coordenador (não pede a hora a ninguém)")
            else:
                add("FALHA", f"{s}: pediu a hora ao coordenador", "nenhum clock_request ENVIADO")
            continue
        off = [c for c in intervals[1:] if c < 15 or c % 15]
        add("OK" if not off else "FALHA", f"{s}: sincroniza a cada 15 mensagens de clientes",
            f"{reqs} pedido(s), {resps} resposta(s); mensagens entre pedidos: {intervals[:12]}")

    # --- qualidade da sincronização (formato do C++; outras linguagens podem não imprimir) ---
    big = []
    seen_any = False
    for s in participants:
        first = True
        for l in lines[s]:
            m = OFFSET.search(l)
            if m:
                seen_any = True
                if first:
                    first = False
                elif abs(int(m.group(2))) > 100:
                    big.append(f"{s}: deslocamento {m.group(2)} ms")
    if seen_any:
        add("OK" if not big else "FALHA", "Depois da 1ª sincronização o deslocamento fica pequeno (≤ 100 ms)", "; ".join(big[:5]))
    else:
        add("?", "Deslocamento do relógio após sincronizar", "nenhuma linha 'deslocamento X ms -> Y ms' no log")

    add("?", "Queda do coordenador", "teste à mão: docker compose stop <coordenador>; esperar ~30 s; conferir novo anúncio e novos clock_request")

    sym = {"OK": "[ OK ]", "FALHA": "[FALHA]", "?": "[ ?  ]"}
    print("Verificação da parte 4 —", path, "\n")
    for st, t, d in results:
        print(f"{sym[st]} {t}" + (f"\n         {d}" if d else ""))
    sys.exit(1 if any(r[0] == "FALHA" for r in results) else 0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])