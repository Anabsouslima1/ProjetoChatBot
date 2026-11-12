package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import chat.proto.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Servidor (REP + PUB + REQ): Processa logins, canais, publicações, relógios lógicos, 
 * heartbeats, eleição (Bully) e replicação de dados (Anti-entropia).
 * 
 * Persistência (por servidor, em DATA_DIR):
 *   records.bin        -> Todos os registros (locais e replicados) com prefixo de tamanho e ReplicaRecord serializado.
 *   logins.tsv         -> <timestamp_ms>\t<usuario>
 *   channels.txt       -> Um canal por linha
 *   publications.bin   -> Prefixado por tamanho + Envelope serializado
 *
 * Utilitário de leitura: java -cp app.jar chat.java.Server --dump /data/publications.bin
 */
public class Server {
    private static final Pattern NOME_VALIDO = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    private static final int MAX_MENSAGEM = 1000;

    private static String dataDir;
    private static final Object travaCarimboELog = new Object();

    private static final AtomicLong relogioLogico = new AtomicLong(0);
    private static final AtomicLong offsetMs = new AtomicLong(longEnv("CLOCK_SKEW_MS", 0));
    private static long ultimaHora = 0;

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--dump")) {
            System.exit(dump(args[1]));
        }

        String name = System.getenv().getOrDefault("SERVER_NAME", "server_java");
        String backend = System.getenv().getOrDefault("BROKER_BACKEND", "tcp://broker:5556");
        String proxyXsub = System.getenv().getOrDefault("PROXY_XSUB", "tcp://proxy:5557");
        String proxyXpub = System.getenv().getOrDefault("PROXY_XPUB", "tcp://proxy:5558");
        String refAddr = System.getenv().getOrDefault("REFERENCE_ADDR", "tcp://reference:5559");
        dataDir = System.getenv().getOrDefault("DATA_DIR", "/data");
        
        int heartbeatCada = (int) Math.max(1, longEnv("HEARTBEAT_EVERY", 10));
        int sincCada = (int) Math.max(1, longEnv("CLOCK_SYNC_EVERY", 15));
        int intervaloSincronizacaoMs = (int) Math.max(500, longEnv("SYNC_INTERVAL_MS", 10000));

        Files.createDirectories(Paths.get(dataDir));
        Replica replica = new Replica(dataDir, name);

        try (ZContext context = new ZContext()) {
            Cluster cl = new Cluster();
            iniciarThread("propagacao", () -> propagarRegistros(context, name, replica, cl));
            iniciarThread("anti-entropia", () -> antiEntropia(context, name, replica, cl, intervaloSincronizacaoMs));
            Referencia ref = new Referencia(context, refAddr, name);
            No no = new No(context, name, ref, cl, heartbeatCada, sincCada);

            while (!no.cadastrar()) {
                System.out.println("[" + name + "] aguardando serviço de referência (retry em 2s)");
                Thread.sleep(2000);
            }

            final int meuRank = no.rank;
            iniciarThread("pares", () -> servirPares(context, name, meuRank, cl, replica));
            iniciarThread("coordenador", () -> escutarCoordenador(name, proxyXpub, cl));
            no.atualizarPares();

            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.setReceiveTimeOut(200);
            socket.connect(backend);
            
            ZMQ.Socket pubSocket = context.createSocket(SocketType.PUB);
            pubSocket.connect(proxyXsub);
            no.pub = pubSocket;
            
            System.out.println("[" + name + "] conectado ao broker em " + backend + " e ao proxy Pub/Sub em "
                    + proxyXsub + " | referência em " + refAddr + " | dados em " + dataDir
                    + " | registros carregados: " + replica.estatisticas().total);

            while (!Thread.currentThread().isInterrupted()) {
                byte[] reqBytes = socket.recv(0);
                if (reqBytes == null) {
                    no.manter();
                    continue;
                }

                Envelope.Builder repBuilder = Envelope.newBuilder().setSender(name);
                try {
                    Envelope req = Envelope.parseFrom(reqBytes);
                    registrarRecebimento(name, req);

                    switch (req.getBodyCase()) {
                        case LOGIN_REQUEST: {
                            String user = req.getLoginRequest().getUsername();
                            if (!nomeValido(user)) {
                                repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_INVALID_USERNAME)
                                        .setErrorMessage("usuário deve ter 1-32 caracteres [A-Za-z0-9_-]"));
                            } else if (!replica.registrarLogin(user, agora())) {
                                repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_INTERNAL)
                                        .setErrorMessage("falha ao gravar login em disco"));
                            } else {
                                repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                        .setSuccess(true).setError(ErrorCode.ERROR_NONE));
                            }
                            break;
                        }

                        case CREATE_CHANNEL_REQUEST: {
                            String canal = req.getCreateChannelRequest().getChannelName();
                            if (!nomeValido(canal)) {
                                repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_INVALID_CHANNEL_NAME)
                                        .setErrorMessage("canal deve ter 1-32 caracteres [A-Za-z0-9_-]"));
                            } else {
                                int resultado = replica.registrarCanal(canal);
                                if (resultado == 0) {
                                    repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                            .setSuccess(true).setError(ErrorCode.ERROR_NONE));
                                } else if (resultado == 1) {
                                    repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                            .setSuccess(false)
                                            .setError(ErrorCode.ERROR_CHANNEL_EXISTS)
                                            .setErrorMessage("canal já existe"));
                                } else {
                                    repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                            .setSuccess(false)
                                            .setError(ErrorCode.ERROR_INTERNAL)
                                            .setErrorMessage("falha ao gravar canal em disco"));
                                }
                            }
                            break;
                        }

                        case LIST_CHANNELS_REQUEST: {
                            repBuilder.setListChannelsResponse(ListChannelsResponse.newBuilder()
                                    .addAllChannels(replica.canais()));
                            break;
                        }

                        case PUBLISH_REQUEST: {
                            String canal = req.getPublishRequest().getChannel();
                            String texto = req.getPublishRequest().getMessage();

                            if (!nomeValido(canal)) {
                                repBuilder.setPublishResponse(PublishResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_INVALID_CHANNEL_NAME)
                                        .setErrorMessage("canal deve ter 1-32 caracteres [A-Za-z0-9_-]"));
                            } else if (!mensagemValida(texto)) {
                                repBuilder.setPublishResponse(PublishResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_INVALID_MESSAGE)
                                        .setErrorMessage("mensagem deve ter 1-" + MAX_MENSAGEM + " bytes"));
                            } else {
                                Envelope pubMsg = carimbarERegistrar(Envelope.newBuilder()
                                        .setPublication(Publication.newBuilder()
                                                .setChannel(canal)
                                                .setMessage(texto)
                                                .setAuthor(req.getSender())), name, "PUBLICADO");

                                if (!replica.registrarPublicacao(pubMsg)) {
                                    repBuilder.setPublishResponse(PublishResponse.newBuilder()
                                            .setSuccess(false)
                                            .setError(ErrorCode.ERROR_INTERNAL)
                                            .setErrorMessage("falha ao gravar publicação em disco"));
                                } else {
                                    pubSocket.sendMore(canal);
                                    pubSocket.send(pubMsg.toByteArray(), 0);
                                    repBuilder.setPublishResponse(PublishResponse.newBuilder()
                                            .setSuccess(true).setError(ErrorCode.ERROR_NONE));
                                }
                            }
                            break;
                        }

                        default: {
                            repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                    .setSuccess(false)
                                    .setError(ErrorCode.ERROR_UNKNOWN_REQUEST)
                                    .setErrorMessage("tipo de requisição não suportado"));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[" + name + "] ERRO ao processar mensagem: " + e.getMessage());
                    repBuilder.setLoginResponse(LoginResponse.newBuilder()
                            .setSuccess(false)
                            .setError(ErrorCode.ERROR_UNKNOWN_REQUEST)
                            .setErrorMessage("mensagem não é um Envelope válido"));
                }

                Envelope rep = carimbarERegistrar(repBuilder, name, "ENVIADO");
                socket.send(rep.toByteArray(), 0);

                no.depoisDeMensagemDeCliente();
                no.manter();
            }
        }
    }

    private static boolean nomeValido(String s) {
        return NOME_VALIDO.matcher(s).matches();
    }

    private static boolean mensagemValida(String s) {
        int n = s.getBytes(StandardCharsets.UTF_8).length;
        return n >= 1 && n <= MAX_MENSAGEM;
    }

    private static long longEnv(String key, long padrao) {
        try {
            return Long.parseLong(System.getenv().getOrDefault(key, String.valueOf(padrao)));
        } catch (NumberFormatException e) {
            return padrao;
        }
    }

    private static long passosMs() {
        return System.nanoTime() / 1_000_000;
    }

    private static void iniciarThread(String nome, Runnable r) {
        Thread t = new Thread(r, nome);
        t.setDaemon(true);
        t.start();
    }

    private static void log(String who, String dir, Envelope e) {
        System.out.println(Instant.ofEpochMilli(e.getTimestampMs()) + " | " + who + " | " + dir
                + " | de=" + e.getSender() + " | relogio=" + e.getLogicalClock() + " | " + Descricao.de(e));
    }

    // ---- Gerenciamento Temporal ----

    static synchronized long agora() {
        long t = System.currentTimeMillis() + offsetMs.get();
        if (t < ultimaHora) t = ultimaHora;
        ultimaHora = t;
        return t;
    }

    static long relogioLogicoAtual() {
        return relogioLogico.get();
    }

    private static long proximoRelogioLogico() {
        return relogioLogico.incrementAndGet();
    }

    private static void atualizarRelogioLogico(long recebido) {
        relogioLogico.accumulateAndGet(recebido, Math::max);
    }

    private static Envelope carimbar(Envelope.Builder b, String who) {
        return b.setSender(who).setTimestampMs(agora()).setLogicalClock(proximoRelogioLogico()).build();
    }

    private static Envelope carimbarERegistrar(Envelope.Builder b, String who, String dir) {
        synchronized (travaCarimboELog) {
            Envelope e = carimbar(b, who);
            log(who, dir, e);
            return e;
        }
    }

    private static void registrarRecebimento(String who, Envelope e) {
        synchronized (travaCarimboELog) {
            atualizarRelogioLogico(e.getLogicalClock());
            log(who, "RECEBIDO", e);
        }
    }

    // ---- Algoritmos Distribuídos (Bully, Cristian e Sincronização) ----

    private static final class Par {
        final String nome;
        final int rank;

        Par(String nome, int rank) {
            this.nome = nome;
            this.rank = rank;
        }
    }

    private static final class Cluster {
        private String coordenador = "";
        private final Map<String, Integer> pares = new LinkedHashMap<>();
        volatile int rank = 0;
        final AtomicBoolean eleicaoPedida = new AtomicBoolean(false);
        final AtomicLong esperarAteMs = new AtomicLong(0);

        synchronized String getCoordenador() {
            return coordenador;
        }

        synchronized void setCoordenador(String c) {
            coordenador = c;
        }

        synchronized void setPares(List<Par> lista) {
            pares.clear();
            for (Par p : lista) pares.put(p.nome, p.rank);
        }

        synchronized int rankDe(String nome) {
            Integer r = pares.get(nome);
            return r == null ? -1 : r;
        }

        synchronized List<Par> todosOsPares() {
            List<Par> out = new ArrayList<>();
            for (Map.Entry<String, Integer> e : pares.entrySet()) {
                out.add(new Par(e.getKey(), e.getValue()));
            }
            return out;
        }

        synchronized List<Par> maiores(int meuRank) {
            List<Par> out = new ArrayList<>();
            for (Map.Entry<String, Integer> e : pares.entrySet()) {
                if (e.getValue() > meuRank) out.add(new Par(e.getKey(), e.getValue()));
            }
            return out;
        }
    }

    private static int portaBase() {
        return (int) longEnv("PEER_PORT_BASE", 5560);
    }

    private static String enderecoPar(String nomeServidor, int rank) {
        String host = System.getenv().getOrDefault("PEER_HOST", nomeServidor);
        return "tcp://" + host + ":" + (portaBase() + rank);
    }

    private static final class Resposta {
        Envelope rep;
        double rttMs;
        long localAoReceber;
    }

    private static Resposta pedirAoPar(ZContext ctx, String who, String ep, Envelope.Builder b, int timeoutMs) {
        Envelope req = carimbarERegistrar(b, who, "ENVIADO");
        ZMQ.Socket s = null;
        try {
            s = ctx.createSocket(SocketType.REQ);
            s.setLinger(0);
            s.setReceiveTimeOut(timeoutMs);
            s.setSendTimeOut(timeoutMs);
            s.connect(ep);
            long t0 = System.nanoTime();
            if (!s.send(req.toByteArray(), 0)) return null;
            byte[] bytes = s.recv(0);
            long t1 = System.nanoTime();
            long localAgora = System.currentTimeMillis();
            if (bytes == null) return null;
            Resposta r = new Resposta();
            r.rep = Envelope.parseFrom(bytes);
            r.rttMs = (t1 - t0) / 1e6;
            r.localAoReceber = localAgora;
            registrarRecebimento(who, r.rep);
            return r;
        } catch (Exception e) {
            System.out.println("[" + who + "] falha de comunicação com par " + ep + ": " + e.getMessage());
            return null;
        } finally {
            if (s != null) ctx.destroySocket(s);
        }
    }

    private static boolean perguntarMaiores(ZContext ctx, String who, int meuRank, List<Par> maiores, int timeoutMs) {
        List<ZMQ.Socket> socks = new ArrayList<>();
        try {
            for (Par h : maiores) {
                ZMQ.Socket s = ctx.createSocket(SocketType.REQ);
                try {
                    s.setLinger(0);
                    s.connect(enderecoPar(h.nome, h.rank));
                    Envelope req = carimbarERegistrar(Envelope.newBuilder().setElectionRequest(
                            ElectionRequest.newBuilder().setName(who).setRank(meuRank)), who, "ENVIADO");
                    s.send(req.toByteArray(), ZMQ.DONTWAIT);
                    socks.add(s);
                } catch (Exception e) {
                    ctx.destroySocket(s);
                }
            }
            boolean[] feito = new boolean[socks.size()];
            long limite = passosMs() + timeoutMs;
            while (passosMs() < limite) {
                int pendentes = 0;
                for (int i = 0; i < socks.size(); i++) {
                    if (feito[i]) continue;
                    byte[] bytes = socks.get(i).recv(ZMQ.DONTWAIT);
                    if (bytes == null) {
                        pendentes++;
                        continue;
                    }
                    feito[i] = true;
                    try {
                        Envelope rep = Envelope.parseFrom(bytes);
                        registrarRecebimento(who, rep);
                        if (rep.hasElectionResponse() && rep.getElectionResponse().getOk()) return true;
                    } catch (Exception e) {
                        // Resposta inválida ignorada
                    }
                }
                if (pendentes == 0) return false;
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return false;
        } finally {
            for (ZMQ.Socket s : socks) ctx.destroySocket(s);
        }
    }

    private static void servirPares(ZContext ctx, String who, int rank, Cluster cl, Replica replica) {
        String bind = "tcp://*:" + (portaBase() + rank);
        ZMQ.Socket rep = ctx.createSocket(SocketType.REP);
        try {
            rep.setLinger(0);
            rep.setReceiveTimeOut(200);
            rep.bind(bind);
        } catch (Exception e) {
            System.out.println("[" + who + "] falha ao realizar bind em " + bind + ": " + e.getMessage());
            return;
        }
        System.out.println("[" + who + "] listeners RPC ativos em " + bind);
        while (!Thread.currentThread().isInterrupted()) {
            byte[] bytes = rep.recv(0);
            if (bytes == null) continue;

            Envelope.Builder respostaBuilder = Envelope.newBuilder().setSender(who)
                    .setElectionResponse(ElectionResponse.newBuilder().setOk(false));
            boolean pedidoRelogio = false;
            try {
                Envelope req = Envelope.parseFrom(bytes);
                registrarRecebimento(who, req);
                if (req.hasClockRequest()) {
                    pedidoRelogio = true;
                } else if (req.hasElectionRequest()) {
                    boolean ok = req.getElectionRequest().getRank() < rank;
                    respostaBuilder.setElectionResponse(ElectionResponse.newBuilder().setOk(ok));
                    if (ok) cl.eleicaoPedida.set(true);
                } else if (req.hasReplicateRequest()) {
                    replica.aplicarRemotos(req.getReplicateRequest().getRecordsList());
                    respostaBuilder.setReplicateResponse(ReplicateResponse.newBuilder().setOk(true));
                } else if (req.hasSyncRequest()) {
                    respostaBuilder.setSyncResponse(responderSincronizacao(replica, req.getSyncRequest()));
                }
            } catch (Exception e) {
                // Em caso de falha no parse, responde default ok=false para liberar lock
            }

            Envelope resposta;
            synchronized (travaCarimboELog) {
                long ts = agora();
                respostaBuilder.setTimestampMs(ts).setLogicalClock(proximoRelogioLogico());
                if (pedidoRelogio) {
                    respostaBuilder.setClockResponse(ClockResponse.newBuilder().setTimeMs(ts));
                }
                resposta = respostaBuilder.build();
                log(who, "ENVIADO", resposta);
            }
            rep.send(resposta.toByteArray(), 0);
        }
    }

    private static SyncResponse responderSincronizacao(Replica replica, SyncRequest pedido) {
        Map<String, Long> tem = new HashMap<>();
        for (VersionEntry v : pedido.getHaveList()) {
            tem.put(v.getOrigin(), v.getLastSeq());
        }
        int maximo = pedido.getMaxRecords() > 0 ? pedido.getMaxRecords() : 500;
        Replica.Faltantes faltantes = replica.faltantes(tem, maximo);
        return SyncResponse.newBuilder()
                .addAllRecords(faltantes.registros)
                .setMore(faltantes.maisRegistros)
                .build();
    }

    private static void escutarCoordenador(String who, String xpub, Cluster cl) {
        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket sub = ctx.createSocket(SocketType.SUB);
            sub.setLinger(0);
            sub.setReceiveTimeOut(500);
            sub.subscribe("servers");
            sub.connect(xpub);
            while (!Thread.currentThread().isInterrupted()) {
                byte[] topico = sub.recv(0);
                if (topico == null) continue;
                if (!sub.hasReceiveMore()) continue;
                byte[] corpo = sub.recv(0);
                if (corpo == null) continue;
                if (!"servers".equals(new String(topico, StandardCharsets.UTF_8))) continue;
                Envelope e;
                try {
                    e = Envelope.parseFrom(corpo);
                } catch (Exception ex) {
                    continue;
                }
                if (!e.hasCoordinatorAnnouncement()) continue;
                registrarRecebimento(who, e);
                String coord = e.getCoordinatorAnnouncement().getName();
                if (coord.equals(who)) continue;
                String atual = cl.getCoordenador();
                int rc = cl.rankDe(coord);
                if (atual.equals(who) && rc >= 0 && rc < cl.rank) {
                    cl.eleicaoPedida.set(true);
                    continue;
                }
                cl.setCoordenador(coord);
                if (!coord.equals(atual)) {
                    System.out.println("[" + who + "] topologia atualizada - coordenador atual: " + coord);
                }
            }
        }
    }

    private static void propagarRegistros(ZContext ctx, String who, Replica replica, Cluster cl) {
        Map<String, Long> pularAteMs = new HashMap<>();
        while (!Thread.currentThread().isInterrupted()) {
            List<ReplicaRecord> lote;
            try {
                lote = replica.esperarFilaDeSaida(500, 100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (lote.isEmpty()) continue;
            for (Par par : cl.todosOsPares()) {
                if (par.nome.equals(who)) continue;
                if (passosMs() < pularAteMs.getOrDefault(par.nome, 0L)) continue;
                Resposta r = pedirAoPar(ctx, who, enderecoPar(par.nome, par.rank), Envelope.newBuilder()
                        .setReplicateRequest(ReplicateRequest.newBuilder().addAllRecords(lote)), 500);
                if (r == null || !r.rep.hasReplicateResponse() || !r.rep.getReplicateResponse().getOk()) {
                    pularAteMs.put(par.nome, passosMs() + 5000);
                }
            }
        }
    }

    private static void antiEntropia(ZContext ctx, String who, Replica replica, Cluster cl, int intervaloMs) {
        try {
            Thread.sleep(3000);
            while (!Thread.currentThread().isInterrupted()) {
                int buscados = 0;
                int alcancados = 0;
                for (Par par : cl.todosOsPares()) {
                    if (par.nome.equals(who)) continue;
                    for (int rodada = 0; rodada < 50; rodada++) {
                        SyncRequest.Builder pedido = SyncRequest.newBuilder().setMaxRecords(500);
                        for (Map.Entry<String, Long> v : replica.versao().entrySet()) {
                            pedido.addHave(VersionEntry.newBuilder().setOrigin(v.getKey()).setLastSeq(v.getValue()));
                        }
                        Resposta r = pedirAoPar(ctx, who, enderecoPar(par.nome, par.rank),
                                Envelope.newBuilder().setSyncRequest(pedido), 2000);
                        if (r == null || !r.rep.hasSyncResponse()) break;
                        if (rodada == 0) alcancados++;
                        buscados += replica.aplicarRemotos(r.rep.getSyncResponse().getRecordsList());
                        if (!r.rep.getSyncResponse().getMore()) break;
                    }
                }
                Replica.Estatisticas e = replica.estatisticas();
                System.out.println("[" + who + "] réplica: registros=" + e.total + " (logins=" + e.logins
        + ", canais=" + e.canais + ", publicações=" + e.publicacoes + ") hash=" + e.hash
        + " | sincronizou com " + alcancados + " servidor(es), " + buscados + " registro(s) novo(s)");
                Thread.sleep(intervaloMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class No {
        final ZContext ctx;
        final String name;
        final Referencia ref;
        final Cluster cl;
        final int heartbeatCada;
        final int sincCada;
        ZMQ.Socket pub;
        int rank = 0;
        boolean registrado = false;
        int desdeHeartbeat = 0;
        int desdeSinc = 0;

        No(ZContext ctx, String name, Referencia ref, Cluster cl, int heartbeatCada, int sincCada) {
            this.ctx = ctx;
            this.name = name;
            this.ref = ref;
            this.cl = cl;
            this.heartbeatCada = heartbeatCada;
            this.sincCada = sincCada;
        }

        boolean cadastrar() {
            Integer r = ref.registrar();
            if (r == null) return false;
            registrado = true;
            rank = r;
            cl.rank = r;
            System.out.println("[" + name + "] registro em control plane finalizado (rank: " + r + ")");
            return true;
        }

        boolean atualizarPares() {
            List<Par> lista = ref.listar();
            if (lista == null) return false;
            cl.setPares(lista);
            return true;
        }

        void anunciar() {
            Envelope e = carimbarERegistrar(Envelope.newBuilder().setCoordinatorAnnouncement(
                    CoordinatorAnnouncement.newBuilder().setName(name)), name, "PUBLICADO");
            pub.sendMore("servers");
            pub.send(e.toByteArray(), 0);
        }

        void eleicao() {
            atualizarPares();
            List<Par> maiores = cl.maiores(rank);
            System.out.println("[" + name + "] iniciando Bully Election (rank " + rank + "): consultando "
                    + maiores.size() + " nó(s) superior(es)");
            if (!maiores.isEmpty() && perguntarMaiores(ctx, name, rank, maiores, 1000)) {
                cl.setCoordenador("");
                cl.esperarAteMs.set(passosMs() + 4000);
                System.out.println("[" + name + "] ACK recebido de nó superior; aguardando anúncio");
            } else {
                cl.setCoordenador(name);
                System.out.println("[" + name + "] assumindo coordenação do cluster");
                anunciar();
            }
        }

        void manter() {
            if (!registrado) return;
            boolean pedida = cl.eleicaoPedida.getAndSet(false);
            boolean nenhum = cl.getCoordenador().isEmpty() && passosMs() >= cl.esperarAteMs.get();
            if (pedida || nenhum) eleicao();
        }

        void sincronizar() {
            if (!registrado) return;
            String coord = cl.getCoordenador();
            if (coord.isEmpty()) return;
            if (coord.equals(name)) {
                anunciar();
                return;
            }
            int crank = cl.rankDe(coord);
            if (crank < 0) {
                atualizarPares();
                crank = cl.rankDe(coord);
            }
            Resposta r = null;
            if (crank >= 0) {
                r = pedirAoPar(ctx, name, enderecoPar(coord, crank),
                        Envelope.newBuilder().setClockRequest(ClockRequest.newBuilder()), 1000);
            }
            if (r != null && r.rep.hasClockResponse()) {
                aplicarSincronizacao(name, coord, r.rep.getClockResponse().getTimeMs(), r.rttMs, r.localAoReceber);
            } else {
                System.out.println("[" + name + "] falha na sincronização de relógio; iniciando eleição");
                cl.setCoordenador("");
                eleicao();
            }
        }

        void depoisDeMensagemDeCliente() {
            if (++desdeSinc >= sincCada) {
                desdeSinc = 0;
                sincronizar();
            }
            if (++desdeHeartbeat < heartbeatCada) return;
            desdeHeartbeat = 0;
            boolean referenciaOk = false;
            if (!registrado) {
                referenciaOk = cadastrar();
            } else {
                int hb = ref.heartbeat();
                if (hb == 1) {
                    referenciaOk = true;
                } else if (hb == -1) {
                    System.out.println("[" + name + "] serviço de referência reportou nó desconhecido; recadastrando");
                    registrado = false;
                    referenciaOk = cadastrar();
                }
            }
            if (referenciaOk) atualizarPares();
        }
    }

    private static void aplicarSincronizacao(String who, String de, long horaRemota, double rttMs, long localAoReceber) {
        final double maxRttMs = 500.0;
        if (rttMs > maxRttMs) {
            System.out.println("[" + who + "] algoritmo de Cristian abortado: limite de latência excedido (RTT "
                    + String.format(Locale.ROOT, "%.1f", rttMs) + " ms)");
            return;
        }
        long estimada = horaRemota + Math.round(rttMs / 2.0);
        long novoOffset = estimada - localAoReceber;
        long antigo = offsetMs.getAndSet(novoOffset);
        System.out.println("[" + who + "] relógio sincronizado com " + de + ": deslocamento " + antigo
        + " ms -> " + novoOffset + " ms (ida e volta: " + String.format(Locale.ROOT, "%.1f", rttMs) + " ms)");
    }

    private static class Referencia {
        private final ZContext context;
        private final String endpoint;
        private final String who;
        private ZMQ.Socket sock;

        Referencia(ZContext context, String endpoint, String who) {
            this.context = context;
            this.endpoint = endpoint;
            this.who = who;
            reset();
        }

        Integer registrar() {
            Envelope r = call(Envelope.newBuilder().setRankRequest(RankRequest.newBuilder().setName(who)));
            return (r != null && r.hasRankResponse()) ? Integer.valueOf(r.getRankResponse().getRank()) : null;
        }

        int heartbeat() {
            Envelope r = call(Envelope.newBuilder().setHeartbeatRequest(HeartbeatRequest.newBuilder().setName(who)));
            if (r == null || !r.hasHeartbeatResponse()) return 0;
            return r.getHeartbeatResponse().getSuccess() ? 1 : -1;
        }

        List<Par> listar() {
            Envelope r = call(Envelope.newBuilder().setListServersRequest(ListServersRequest.newBuilder()));
            if (r == null || !r.hasListServersResponse()) return null;
            List<Par> out = new ArrayList<>();
            for (ServerInfo s : r.getListServersResponse().getServersList()) {
                out.add(new Par(s.getName(), s.getRank()));
            }
            return out;
        }

        private void reset() {
            if (sock != null) {
                context.destroySocket(sock);
                sock = null;
            }
            ZMQ.Socket novo = context.createSocket(SocketType.REQ);
            try {
                novo.setLinger(0);
                novo.setSendTimeOut(3000);
                novo.setReceiveTimeOut(3000);
                novo.connect(endpoint);
                sock = novo;
            } catch (Exception e) {
                System.out.println("[" + who + "] falha de resolução do serviço de referência em "
                        + endpoint + ": " + e.getMessage());
                context.destroySocket(novo);
            }
        }

        private Envelope call(Envelope.Builder builder) {
            if (sock == null) {
                reset();
                if (sock == null) return null;
            }
            Envelope req = carimbarERegistrar(builder, who, "ENVIADO");
            if (!sock.send(req.toByteArray(), 0)) {
                reset();
                return null;
            }
            byte[] bytes = sock.recv(0);
            if (bytes == null) {
                System.out.println("[" + who + "] RPC timeout (Lazy Pirate), forçando recriação do socket para " + endpoint);
                reset();
                return null;
            }
            try {
                Envelope rep = Envelope.parseFrom(bytes);
                registrarRecebimento(who, rep);
                return rep;
            } catch (Exception e) {
                reset();
                return null;
            }
        }
    }

    private static int dump(String path) throws IOException {
        if (!Files.exists(Paths.get(path))) {
            System.err.println("arquivo não encontrado: " + path);
            return 1;
        }
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(Paths.get(path)));
        List<Envelope> publicacoes = new ArrayList<>();
        while (buf.remaining() >= 4) {
            int n = buf.getInt();
            if (n < 0 || n > buf.remaining()) {
                System.err.println("EOF inconsistente: arquivo truncado");
                return 1;
            }
            byte[] rec = new byte[n];
            buf.get(rec);
            publicacoes.add(Envelope.parseFrom(rec));
        }
        publicacoes.sort(Comparator.comparingLong(Envelope::getLogicalClock).thenComparing(Envelope::getSender));
        for (Envelope e : publicacoes) {
            log("dump", "ARMAZENADO", e);
        }
        System.out.println(publicacoes.size() + " publicações processadas (ordem total garantida por Lamport Clock)");
        return 0;
    }
}