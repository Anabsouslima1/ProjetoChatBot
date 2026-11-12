#!/usr/bin/env python3
"""Confere a replicação (parte 5) a partir do log do compose.

Todos os servidores devem terminar com TODOS os registros (logins, canais e publicações).
Como os bots escrevem o tempo todo, o teste precisa de uma pausa no final para a réplica convergir:

    docker compose -f docker-compose.yaml -f docker-compose.host.yaml up -d --build
    sleep 90
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml stop \
    client_cpp_1 client_cpp_2 \
    client_java_1 client_java_2 \
    client_node_1 client_node_2
    sleep 30
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml logs --no-color > run.log
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml down
    python3 scripts/check_parte5.py run.log

Cada servidor imprime periodicamente (padrão, igual em todas as linguagens):
    [servidor] réplica: registros=N (logins=a, canais=b, publicações=c) [hash=H] | ...
O que não dá para saber pelo log aparece como [ ? ].
"""
import re
import sys
from collections import defaultdict

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LINE = re.compile(r"^(?P<svc>[\w.]+?)-\d+\s+\|\s?(?P<msg>.*)$")
STATE = re.compile(r"réplica:\s*registros=(\d+)\s*\(logins=(\d+),\s*canais=(\d+),\s*publica[çc][õo]es=(\d+)\)(?:\s*hash=(\w+))?")
MSG = re.compile(r"\|\s*(ENVIADO|RECEBIDO|PUBLICADO)\s*\|")


def main(path):
    lines = defaultdict(list)
    for raw in open(path, encoding="utf-8", errors="replace"):
        m = LINE.match(ANSI.sub("", raw.rstrip("\n")))
        if m:
            lines[m.group("svc")].append(m.group("msg"))

    servers = sorted(s for s in lines if s.startswith("server_"))
    clients = sorted(s for s in lines if s.startswith("client_"))
    results = []

    def add(status, title, detail=""):
        results.append((status, title, detail))

    # 1) mensagens de replicação trafegaram
    for kind, label in (("replicate_request", "empurra registros novos (replicate_request)"),
                        ("sync_request", "faz anti-entropia (sync_request)")):
        quiet = [s for s in servers if not any(kind in l and "ENVIADO" in l for l in lines[s])]
        add("OK" if not quiet else "FALHA", f"Cada servidor {label}", f"sem: {', '.join(quiet)}" if quiet else "")
    recv = [s for s in servers if not any("replicate_request" in l and "RECEBIDO" in l for l in lines[s])]
    add("OK" if not recv else "FALHA", "Todos os servidores recebem registros dos outros (replicate_request RECEBIDO)",
        f"sem: {', '.join(recv)}" if recv else "")

    # 2) estado final de cada servidor
    final = {}
    for s in servers:
        for l in lines[s]:
            m = STATE.search(l)
            if m:
                final[s] = tuple(int(x) for x in m.groups()[:4]) + (m.group(5),)
    missing = [s for s in servers if s not in final]
    add("OK" if not missing else "FALHA", "Todos os servidores informam o estado da réplica",
        f"sem linha 'réplica: registros=...': {', '.join(missing)}" if missing else "")
    if final:
        counts = {s: v[:4] for s, v in final.items()}
        distinct = set(counts.values())
        detail = "; ".join(f"{s}: registros={v[0]} logins={v[1]} canais={v[2]} publicações={v[3]}" for s, v in sorted(counts.items()))
        add("OK" if len(distinct) == 1 else "FALHA", "Todos os servidores terminam com os mesmos registros (contagens da última linha)", detail)
        hashes = {s: v[4] for s, v in final.items() if v[4]}
        if len(hashes) == len(final) and len(final) > 1:
            add("OK" if len(set(hashes.values())) == 1 else "FALHA", "Mesmo conjunto de registros (hash dos ids)",
                ", ".join(f"{s}={h}" for s, h in sorted(hashes.items())))

        # 3) nenhuma publicação aceita pelos bots foi perdida
        ok_pubs = 0
        for c in clients:
            ok_pubs += sum(1 for l in lines[c] if MSG.search(l) and "RECEBIDO" in l and "publish_response{success=true" in l)
        if ok_pubs:
            short = {s: v[3] for s, v in final.items() if v[3] < ok_pubs}
            add("OK" if not short else "FALHA", "Cada servidor tem todas as publicações aceitas pelos bots",
                f"bots receberam {ok_pubs} confirmações; " +
                (", ".join(f"{s} tem só {n}" for s, n in sorted(short.items())) if short else "todos os servidores têm pelo menos esse total"))
        else:
            add("?", "Cada servidor tem todas as publicações aceitas pelos bots", "nenhuma publish_response de sucesso nos clientes")

        # 4) evolução: a réplica realmente foi crescendo (não ficou parada)
        for s in servers:
            seq = [int(m.group(1)) for l in lines[s] for m in [STATE.search(l)] if m]
            if len(seq) >= 3 and seq[-1] == seq[0]:
                add("FALHA", f"{s}: a réplica cresceu durante o teste", f"sempre {seq[0]} registros")

    add("?", "Servidor que cai e volta recupera o que perdeu",
        "teste à mão: docker compose stop <servidor>; esperar; start; esperar ~30 s; conferir que as contagens voltam a ser iguais")

    sym = {"OK": "[ OK ]", "FALHA": "[FALHA]", "?": "[ ?  ]"}
    print("Verificação da parte 5 —", path, "\n")
    for st, t, d in results:
        print(f"{sym[st]} {t}" + (f"\n         {d}" if d else ""))
    sys.exit(1 if any(r[0] == "FALHA" for r in results) else 0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
