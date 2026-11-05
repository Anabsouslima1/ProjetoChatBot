#!/usr/bin/env python3
"""Confere os requisitos da parte 2 a partir do log do `docker compose up`.

Uso:
    docker compose -f docker-compose.yaml -f docker-compose.host.yaml up --build > run.log 2>&1
    (deixe rodar 1-2 minutos, pare com Ctrl+C)
    python3 scripts/check_parte2.py run.log

Só usa o que aparece no log. O que não dá para saber pelo log aparece como [ ? ].
"""
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from statistics import median

LANGS = ["cpp", "java", "node"]
SERVERS = [f"server_{l}_{i}" for l in LANGS for i in (1, 2)]
CLIENTS = [f"client_{l}_{i}" for l in LANGS for i in (1, 2)]
EXPECTED = ["broker", "proxy"] + SERVERS + CLIENTS

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LINE = re.compile(r"^(?P<svc>[\w.]+?)-\d+\s+\|\s?(?P<msg>.*)$")
TS = re.compile(r"(\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d(?:\.\d+)?)Z?")
BOT = re.compile(r"bot_(cpp|java|node)_\d")


def to_ms(s):
    dt = datetime.fromisoformat(s.replace(" ", "T")).replace(tzinfo=timezone.utc)
    return dt.timestamp() * 1000


def short(items, n=6):
    items = list(items)
    return ", ".join(items[:n]) + (f" (+{len(items) - n})" if len(items) > n else "")


def main(path):
    lines = defaultdict(list)  # serviço -> linhas
    for raw in open(path, encoding="utf-8", errors="replace"):
        raw = ANSI.sub("", raw.rstrip("\n"))
        m = LINE.match(raw)
        if m:
            lines[m.group("svc")].append(m.group("msg"))

    results = []

    def add(status, title, detail=""):
        results.append((status, title, detail))

    # 1) Todos os containers apareceram?
    missing = [s for s in EXPECTED if s not in lines]
    add("OK" if not missing else "FALHA", "Todos os serviços (broker, proxy, 2 servidores e 2 clientes por linguagem) geraram log",
        f"sem log: {short(missing)}" if missing else f"{len(EXPECTED)} serviços")

    # 2) Servidores ligados ao proxy Pub/Sub (XSUB 5557)
    bad = [s for s in SERVERS if not any("5557" in l for l in lines.get(s, []))]
    add("OK" if not bad else "FALHA", "Servidores conectados ao proxy Pub/Sub (porta 5557)",
        f"sem menção ao 5557: {short(bad)}" if bad else "")

    # 3) Login dos bots
    bad = [c for c in CLIENTS if not any(re.search(r"login.*(sucesso|success)", l, re.I) and "falhou" not in l.lower()
                                         for l in lines.get(c, []))]
    add("OK" if not bad else "FALHA", "Bots fizeram login", f"sem login: {short(bad)}" if bad else "")

    # 4) Quantidade de publicações pedidas por bot
    pubs = {c: [l for l in lines.get(c, []) if "publish_request" in l] for c in CLIENTS}
    counts = {c: len(v) for c, v in pubs.items()}
    zero = [c for c, n in counts.items() if n == 0]
    few = [c for c, n in counts.items() if 0 < n < 10]
    status = "FALHA" if zero else ("?" if few else "OK")
    add(status, "Bots publicam mensagens (publish_request)",
        "publicações por bot: " + ", ".join(f"{c.replace('client_', '')}={n}" for c, n in counts.items())
        + ("  | log curto demais para 10 mensagens em alguns bots" if few and not zero else ""))

    # 5) Intervalo de ~1 s entre mensagens e rodadas de 10 no mesmo canal
    medians, no_ts, bursts_bad, bursts_all = {}, [], [], Counter()
    for c, ls in pubs.items():
        stamps = []
        for l in ls:
            m = TS.search(l)
            if m:
                stamps.append(to_ms(m.group(1)))
        if len(stamps) >= 4:
            deltas = [b - a for a, b in zip(stamps, stamps[1:]) if 0 < b - a <= 3000]
            if deltas:
                medians[c] = median(deltas)
        elif ls:
            no_ts.append(c)
        chans = [(re.search(r"channel(?:_name)?[=:]\s*\"?([\w-]+)", l) or [None, None])[1] for l in ls]
        if chans and all(chans):
            runs, cur = [], 1
            for a, b in zip(chans, chans[1:]):
                if a == b:
                    cur += 1
                else:
                    runs.append(cur)
                    cur = 1
            for r in runs:  # rodadas completas (a última pode estar cortada)
                bursts_all[r] += 1
                if r % 10 != 0:
                    bursts_bad.append((c, r))
    off = {c: v for c, v in medians.items() if not 800 <= v <= 1300}
    if medians:
        add("OK" if not off else "FALHA", "Intervalo de ~1 s entre mensagens do mesmo bot",
            "mediana por bot (ms): " + ", ".join(f"{c.replace('client_', '')}={v:.0f}" for c, v in medians.items())
            + (f"  | sem timestamp no log: {short(no_ts)}" if no_ts else ""))
    else:
        add("?", "Intervalo de ~1 s entre mensagens do mesmo bot", "linhas de publish_request sem timestamp/poucas amostras no log")
    if bursts_all:
        add("OK" if not bursts_bad else "FALHA", "Rodadas de 10 mensagens no mesmo canal",
            "tamanhos das rodadas completas: " + ", ".join(f"{k}x{v}" for k, v in sorted(bursts_all.items()))
            + (f"  | fora do padrão: {short(f'{c}:{n}' for c, n in bursts_bad)}" if bursts_bad else ""))
    else:
        add("?", "Rodadas de 10 mensagens no mesmo canal", "log curto ou formato sem o nome do canal no publish_request")

    # 6) Inscrições (mínimo 3 canais por bot, ao longo das rodadas)
    subs = {c: sum(1 for l in lines.get(c, []) if re.search(r"inscri|subscri", l, re.I)) for c in CLIENTS}
    zero = [c for c, n in subs.items() if n == 0]
    low = [c for c, n in subs.items() if 0 < n < 3]
    add("FALHA" if zero else ("?" if low else "OK"), "Bots se inscrevem em canais (até ter 3 inscrições)",
        "inscrições por bot: " + ", ".join(f"{c.replace('client_', '')}={n}" for c, n in subs.items()))

    # 7) Exibição das mensagens recebidas (canal, texto, timestamp de envio e de recebimento)
    recv = {c: [l for l in lines.get(c, []) if re.search(r"MENSAGEM|recebida_em", l)] for c in CLIENTS}
    none = [c for c, v in recv.items() if not v]
    incomplete = [(c, l) for c, v in recv.items() for l in v
                  if len(TS.findall(l)) < 2 or not re.search(r"canal|channel", l, re.I)]
    status = "FALHA" if none else ("FALHA" if incomplete else "OK")
    add(status, "Bots exibem mensagens recebidas (canal, texto, horário de envio e de recebimento)",
        "mensagens exibidas por bot: " + ", ".join(f"{c.replace('client_', '')}={len(v)}" for c, v in recv.items())
        + (f"  | sem nenhuma: {short(none)}" if none else "")
        + (f"  | linhas sem 2 timestamps ou canal: {len(incomplete)} (ex.: {incomplete[0][0]})" if incomplete else ""))

    # 8) Troca entre linguagens: quem recebe de quem
    matrix = {l: set() for l in LANGS}
    for c, v in recv.items():
        own = "bot_" + c.replace("client_", "")
        lang = c.split("_")[1]
        for l in v:
            for m in BOT.finditer(l):
                if m.group(0) != own:
                    matrix[lang].add(m.group(1))
    gaps = [f"{r}<-{a}" for r in LANGS for a in LANGS if a != r and a not in matrix[r]]
    same = [f"{r}<-{r}" for r in LANGS if r not in matrix[r]]
    add("OK" if not gaps else "FALHA", "Mensagens atravessam linguagens (cada linguagem recebe das outras duas)",
        "recebe de: " + "; ".join(f"{r}: {short(sorted(matrix[r])) or 'ninguém'}" for r in LANGS)
        + (f"  | faltando: {short(gaps, 9)}" if gaps else "")
        + (f"  | sem troca entre bots da mesma linguagem: {short(same)} (depende do sorteio de canais; veja a auditoria abaixo)"
           if same else ""))

    # 8b) Auditoria de entrega: tudo que foi publicado num canal assinado chegou ao assinante?
    SUB = re.compile(r"(?:inscrit|subscri)\w*.*?canal\s+([\w-]+)", re.I)
    CH = re.compile(r"channel(?:_name)?[=:]\s*\"?([\w-]+)")
    subs, end_t = {}, {}
    for c in CLIENTS:
        last, d, end = None, {}, None
        for l in lines.get(c, []):
            m = TS.search(l)
            t = to_ms(m.group(1)) if m else None
            s = SUB.search(l)
            if s:  # a linha de inscrição pode não ter horário: usa o da linha anterior que tem
                d.setdefault(s.group(1), t if t is not None else last)
            elif t is not None:
                last = t
            if t is not None:
                end = t if end is None else max(end, t)
        subs[c], end_t[c] = d, end
    # janela: de 1 s depois de assinar (propagação da inscrição) até 2 s antes do fim do log
    window = {}
    for y in CLIENTS:
        if subs[y] and end_t[y] and all(v is not None for v in subs[y].values()):
            for ch, t0 in subs[y].items():
                window[(y, ch)] = (t0 + 1000, end_t[y] - 2000)
    exp, rec, unsolicited = Counter(), Counter(), Counter()
    for x in CLIENTS:
        for l in pubs[x]:
            m, c = TS.search(l), CH.search(l)
            if not (m and c):
                continue
            t = to_ms(m.group(1))
            for (y, ch), (a, b) in window.items():
                if ch == c.group(1) and a <= t <= b:
                    exp[(y, ch, x)] += 1
    for y in CLIENTS:
        for l in recv[y]:
            c, au = re.search(r"canal=([\w-]+)", l), re.search(r"de=(bot_\w+)", l)
            sm = re.search(r"enviada_em=(\d{4}-\d\d-\d\d[ T][\d:.]+)", l)
            stamps = TS.findall(l)
            sent = to_ms(sm.group(1)) if sm else (to_ms(stamps[1]) if len(stamps) >= 2 else None)
            if not (c and au and sent):
                continue
            ch = c.group(1)
            if subs[y] and ch not in subs[y]:
                unsolicited[y] += 1
            elif (y, ch) in window and window[(y, ch)][0] <= sent <= window[(y, ch)][1]:
                rec[(y, ch, "client_" + au.group(1)[4:])] += 1
    delivered, expected, worst = Counter(), Counter(), []
    for (y, ch, x), e in exp.items():
        r = rec.get((y, ch, x), 0)
        expected[y] += e
        delivered[y] += min(r, e)
        if r < e:
            worst.append((e - r, y, ch, x, r, e))
    worst.sort(reverse=True)
    if not expected:
        add("?", "Entrega: o que foi publicado num canal assinado chegou ao assinante",
            "sem horários/inscrições suficientes no log para auditar")
    else:
        bad = [y for y in expected if delivered[y] / expected[y] < 0.98]
        add("FALHA" if bad else "OK", "Entrega: o que foi publicado num canal assinado chegou ao assinante",
            "entregue/esperado por bot: " + ", ".join(
                f"{y.replace('client_', '')}={delivered[y]}/{expected[y]}" for y in CLIENTS if y in expected)
            + ("  | piores casos: " + short(
                f"{x.replace('client_', '')}->{ch} em {y.replace('client_', '')}: {r}/{e}"
                for _, y, ch, x, r, e in worst) if worst else ""))
    add("FALHA" if unsolicited else "OK", "Bots só exibem mensagens de canais em que se inscreveram",
        ", ".join(f"{y.replace('client_', '')}={n}" for y, n in unsolicited.items())
        if unsolicited else "nenhuma mensagem de canal não assinado")

    # 9) Servidores publicam
    pubd = {s: sum(1 for l in lines.get(s, []) if re.search(r"PUBLICADO|publication", l, re.I)) for s in SERVERS}
    zero = [s for s, n in pubd.items() if n == 0]
    add("OK" if not zero else "?", "Servidores publicaram no Pub/Sub",
        "publicações por servidor: " + ", ".join(f"{s.replace('server_', '')}={n}" for s, n in pubd.items())
        + ("  | zero pode ser só formato de log diferente ou o broker não ter sorteado o servidor" if zero else ""))

    # 10) Códigos de erro inesperados nas respostas
    errs = Counter()
    for svc in CLIENTS:
        for l in lines.get(svc, []):
            for e in re.findall(r"erro=(ERROR_\w+)", l):
                errs[e] += 1
    serious = {e: n for e, n in errs.items() if e in ("ERROR_INTERNAL", "ERROR_UNKNOWN_REQUEST")}
    add("FALHA" if serious else "OK", "Respostas de erro vistas pelos bots",
        ", ".join(f"{e}={n}" for e, n in errs.items()) or "nenhuma")

    # 11) Quedas e exceções
    problems = defaultdict(list)
    for svc, ls in lines.items():
        for l in ls:
            if re.search(r"Traceback|Exception|Segmentation|terminate called|exited with code (?!0\b|143\b)|ECONN|EADDRINUSE|Address already in use", l):
                problems[svc].append(l.strip()[:100])
    add("OK" if not problems else "FALHA", "Sem exceções, quedas ou porta em uso",
        "; ".join(f"{s}: {v[0]}" for s, v in list(problems.items())[:4]) if problems else "")
    timeouts = {s: sum(1 for l in ls if "sem resposta" in l.lower()) for s, ls in lines.items()}
    timeouts = {s: n for s, n in timeouts.items() if n}
    if timeouts:
        add("?", "Bots que estouraram o tempo de resposta (esperado ao parar containers)",
            ", ".join(f"{s}={n}" for s, n in timeouts.items()))

    # Saída
    mark = {"OK": "[ OK  ]", "FALHA": "[FALHA]", "?": "[  ?  ]"}
    print(f"\nVerificação da parte 2 — {path}\n")
    for st, title, detail in results:
        print(f"{mark[st]} {title}")
        if detail:
            print(f"         {detail}")
    print("\nNão dá para ver pelo log (confira à mão):")
    print("  - publicações gravadas em disco por TODOS os servidores (C++, Java e Node.js), em volume próprio")
    print("      C++: docker compose exec server_cpp_1 /app/server --dump /data/publications.bin | tail -3")
    print("      Java/Node: ls -la data/ e abrir o arquivo de publicações")
    print("  - o README descreve o formato de armazenamento das publicações de cada linguagem")
    print("  - um único proxy no docker-compose.yaml, com 5557 (XSUB) e 5558 (XPUB)")
    return 1 if any(r[0] == "FALHA" for r in results) else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1]))