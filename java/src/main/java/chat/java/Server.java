package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import chat.proto.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

// Servidor (REP + PUB + REQ para a referência): login, canais, publicações, relógios e heartbeat.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv        -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt      -> um canal por linha
//   publications.bin  -> cada publicação: 4 bytes (big-endian) com o tamanho + Envelope serializado
//                        (mesmo formato dos servidores C++ e Node)
//
// Parte 3:
//   - relógio lógico (contador): incrementado antes de cada envio e enviado no Envelope;
//     ao receber, vira o máximo entre o valor local e o recebido.
//   - ao subir, pede o rank à referência (isso cadastra o servidor);
//     a cada HEARTBEAT_EVERY (10) mensagens de clientes, envia heartbeat à referência.
//
// Parte 4 (a hora deixa de vir da referência; 
//   - um servidor é o coordenador (eleição pelo algoritmo do valentão/bully: vence o maior rank);
//   - a cada CLOCK_SYNC_EVERY (15) mensagens de clientes, o servidor pede a hora ao coordenador
//     (REQ clock_request / REP clock_response) e ajusta o relógio físico;
//   - se o coordenador não responde (ou ainda não há um), inicia eleição: pede eleição aos servidores
//     de rank maior (REQ election_request / REP OK); se ninguém de rank maior responde, assume e
//     publica o nome no tópico "servers"; todos assinam esse tópico e guardam o coordenador.
//   - cada servidor atende os pedidos dos outros em tcp://*:(PEER_PORT_BASE + rank). O endereço de um
//     par é tcp://<PEER_HOST ou nome do servidor>:(PEER_PORT_BASE + rank do par).
// Para listar publications.bin:  java -cp app.jar chat.java.Server --dump /data/publications.bin
public class Server {
    private static final Pattern NOME_VALIDO = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    private static final int MAX_MENSAGEM = 1000; // bytes

    private static Set<String> canais = new HashSet<>();
    private static String dataDir;

    // Relógio lógico: usado pela thread principal, pela que atende os outros servidores e pela do SUB
    private static final AtomicLong relogioLogico = new AtomicLong(0);
    // Deslocamento do relógio físico em relação ao do coordenador (ms).
    // CLOCK_SKEW_MS: só para testes (simula uma máquina com o relógio adiantado/atrasado).
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
        int heartbeatCada = (int) Math.max(1, longEnv("HEARTBEAT_EVERY", 10)); // enunciado: 10 mensagens
        int sincCada = (int) Math.max(1, longEnv("CLOCK_SYNC_EVERY", 15));      // enunciado (Parte 4): 15

        Files.createDirectories(Paths.get(dataDir));
        carregarCanais();

        try (ZContext context = new ZContext()) {
            Cluster cl = new Cluster();
            Referencia ref = new Referencia(context, refAddr, name);
            No no = new No(context, name, ref, cl, heartbeatCada, sincCada);

            // Primeiro o servidor se registra na referência (rank). Só depois conecta ao broker,
            // para o broker não mandar requisições de bots a um servidor que ainda não está pronto.
            while (!no.cadastrar()) {
                System.out.println("[" + name + "] sem resposta da referência, tentando novamente em 2s");
                Thread.sleep(2000);
            }

            // Threads da Parte 4: atende os outros servidores e escuta o tópico "servers"
            final int meuRank = no.rank;
            iniciarThread("pares", () -> servirPares(context, name, meuRank, cl));
            iniciarThread("coordenador", () -> escutarCoordenador(name, proxyXpub, cl));
            no.atualizarPares();

            // REP: requisições dos bots (via broker). Acorda a cada 200 ms para cuidar de eleições
            // mesmo sem clientes.
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.setReceiveTimeOut(200);
            socket.connect(backend);
            // PUB: publicações nos canais e anúncios do coordenador (via proxy)
            ZMQ.Socket pubSocket = context.createSocket(SocketType.PUB);
            pubSocket.connect(proxyXsub);
            no.pub = pubSocket;
            System.out.println("[" + name + "] conectado ao broker em " + backend + " e ao proxy Pub/Sub em "
                    + proxyXsub + " | referência em " + refAddr + " | dados em " + dataDir
                    + " | canais carregados: " + canais.size());

            while (!Thread.currentThread().isInterrupted()) {
                byte[] reqBytes = socket.recv(0);
                if (reqBytes == null) {
                    no.manter();
                    continue;
                }

                Envelope.Builder repBuilder = Envelope.newBuilder().setSender(name);
                try {
                    Envelope req = Envelope.parseFrom(reqBytes);
                    atualizarRelogioLogico(req.getLogicalClock());
                    log(name, "RECEBIDO", req);

                    switch (req.getBodyCase()) {
                        case LOGIN_REQUEST: {
                            String user = req.getLoginRequest().getUsername();
                            if (!nomeValido(user)) {
                                repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_INVALID_USERNAME)
                                        .setErrorMessage("usuário deve ter 1-32 caracteres [A-Za-z0-9_-]"));
                            } else {
                                guardarLogin(user, agora());
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
                            } else if (canais.contains(canal)) {
                                repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_CHANNEL_EXISTS)
                                        .setErrorMessage("canal já existe"));
                            } else {
                                guardarCanal(canal);
                                canais.add(canal);
                                repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                        .setSuccess(true).setError(ErrorCode.ERROR_NONE));
                            }
                            break;
                        }

                        case LIST_CHANNELS_REQUEST: {
                            repBuilder.setListChannelsResponse(ListChannelsResponse.newBuilder()
                                    .addAllChannels(canais));
                            break;
                        }

                        case PUBLISH_REQUEST: {
                            String canal = req.getPublishRequest().getChannel();
                            String texto = req.getPublishRequest().getMessage();

                            // Cada servidor conhece só os canais criados nele (dados separados), então
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
                               
                                Envelope pubMsg = Envelope.newBuilder()
                                        .setTimestampMs(agora())
                                        .setLogicalClock(proximoRelogioLogico())
                                        .setSender(name)
                                        .setPublication(Publication.newBuilder()
                                                .setChannel(canal)
                                                .setMessage(texto)
                                                .setAuthor(req.getSender()))
                                        .build();

                                boolean gravou = true;
                                try {
                                    guardarPublicacao(pubMsg);
                                } catch (IOException e) {
                                    gravou = false;
                                }

                                if (!gravou) {
                                    repBuilder.setPublishResponse(PublishResponse.newBuilder()
                                            .setSuccess(false)
                                            .setError(ErrorCode.ERROR_INTERNAL)
                                            .setErrorMessage("falha ao gravar publicação em disco"));
                                } else {
                                    // PUB: frame 1 = tópico (nome do canal), frame 2 = Envelope
                                    pubSocket.sendMore(canal);
                                    pubSocket.send(pubMsg.toByteArray(), 0);
                                    log(name, "PUBLICADO", pubMsg);
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

                // REP precisa sempre responder, senão o socket trava
                repBuilder.setTimestampMs(agora()).setLogicalClock(proximoRelogioLogico());
                Envelope rep = repBuilder.build();
                socket.send(rep.toByteArray(), 0);
                log(name, "ENVIADO", rep);

                // Heartbeat (10) e sincronização do relógio (15), depois de responder ao bot
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

    // ---- Relógio físico (corrigido pelo coordenador) e relógio lógico ----

    // Hora atual do servidor já corrigida pelo deslocamento. Nunca retrocede, mesmo que o
    // deslocamento diminua numa sincronização.
    private static synchronized long agora() {
        long t = System.currentTimeMillis() + offsetMs.get();
        if (t < ultimaHora) t = ultimaHora;
        ultimaHora = t;
        return t;
    }

    // Regra 1: incrementa antes de enviar e devolve o valor para ir na mensagem
    private static long proximoRelogioLogico() {
        return relogioLogico.incrementAndGet();
    }

    // Regra 2: ao receber, usa o máximo entre o valor local e o recebido
    private static void atualizarRelogioLogico(long recebido) {
        relogioLogico.accumulateAndGet(recebido, Math::max);
    }

    // Carimba a mensagem imediatamente antes do envio: remetente, hora (corrigida) e relógio lógico
    private static Envelope carimbar(Envelope.Builder b, String who) {
        return b.setSender(who).setTimestampMs(agora()).setLogicalClock(proximoRelogioLogico()).build();
    }

    // ---- Parte 4: comunicação entre servidores (coordenador, eleição, sincronização) ----

    private static final class Par {
        final String nome;
        final int rank;

        Par(String nome, int rank) {
            this.nome = nome;
            this.rank = rank;
        }
    }

    // Estado compartilhado entre a thread principal, a que atende os outros servidores (REP)
    // e a que escuta o tópico "servers" (SUB).
    private static final class Cluster {
        private String coordenador = ""; // vazio = ainda não há coordenador conhecido
        private final Map<String, Integer> pares = new LinkedHashMap<>(); // nome -> rank de todos os servidores
        volatile int rank = 0;
        final AtomicBoolean eleicaoPedida = new AtomicBoolean(false); // um servidor de rank menor pediu eleição
        final AtomicLong esperarAteMs = new AtomicLong(0);            // após perder uma eleição, espera o anúncio até aqui

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
        double rttMs;       // tempo entre o send e o recv
        long localAoReceber; // hora da máquina (sem correção) no instante do recv
    }

    // REQ avulso a outro servidor (um socket novo por chamada: se o par caiu, nada fica travado).
    // Mede o RTT só entre o send e o recv. Retorna null se o par não respondeu.
    private static Resposta pedirAoPar(ZContext ctx, String who, String ep, Envelope.Builder b, int timeoutMs) {
        Envelope req = carimbar(b, who);
        log(who, "ENVIADO", req);
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
            atualizarRelogioLogico(r.rep.getLogicalClock());
            log(who, "RECEBIDO", r.rep);
            return r;
        } catch (Exception e) {
            System.out.println("[" + who + "] erro ao falar com " + ep + ": " + e.getMessage());
            return null;
        } finally {
            if (s != null) ctx.destroySocket(s);
        }
    }

    // Pede eleição a todos os servidores de rank maior ao mesmo tempo. Retorna true se algum respondeu OK.
    private static boolean perguntarMaiores(ZContext ctx, String who, int meuRank, List<Par> maiores, int timeoutMs) {
        List<ZMQ.Socket> socks = new ArrayList<>();
        try {
            for (Par h : maiores) {
                ZMQ.Socket s = ctx.createSocket(SocketType.REQ);
                try {
                    s.setLinger(0);
                    s.connect(enderecoPar(h.nome, h.rank));
                    Envelope req = carimbar(Envelope.newBuilder().setElectionRequest(
                            ElectionRequest.newBuilder().setName(who).setRank(meuRank)), who);
                    log(who, "ENVIADO", req);
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
                        atualizarRelogioLogico(rep.getLogicalClock());
                        log(who, "RECEBIDO", rep);
                        if (rep.hasElectionResponse() && rep.getElectionResponse().getOk()) return true;
                    } catch (Exception e) {
                        // resposta inválida: ignora
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

    // Thread: atende os pedidos dos outros servidores (clock_request e election_request).
    private static void servirPares(ZContext ctx, String who, int rank, Cluster cl) {
        String bind = "tcp://*:" + (portaBase() + rank);
        ZMQ.Socket rep = ctx.createSocket(SocketType.REP);
        try {
            rep.setLinger(0);
            rep.setReceiveTimeOut(200);
            rep.bind(bind);
        } catch (Exception e) {
            System.out.println("[" + who + "] não foi possível escutar em " + bind + ": " + e.getMessage());
            return;
        }
        System.out.println("[" + who + "] atendendo outros servidores em " + bind);
        while (!Thread.currentThread().isInterrupted()) {
            byte[] bytes = rep.recv(0);
            if (bytes == null) continue;

            boolean pedidoRelogio = false;
            boolean ok = false;
            try {
                Envelope req = Envelope.parseFrom(bytes);
                atualizarRelogioLogico(req.getLogicalClock());
                log(who, "RECEBIDO", req);
                if (req.hasClockRequest()) {
                    pedidoRelogio = true;
                } else if (req.hasElectionRequest()) {
                    // "OK" só se quem pede tem rank menor; neste caso eu assumo a eleição
                    // (no próximo ciclo da thread principal)
                    ok = req.getElectionRequest().getRank() < rank;
                    if (ok) cl.eleicaoPedida.set(true);
                }
            } catch (Exception e) {
                // mensagem inválida: responde "não" para o REP não travar
            }

            long ts = agora();
            Envelope.Builder b = Envelope.newBuilder().setSender(who).setTimestampMs(ts)
                    .setLogicalClock(proximoRelogioLogico());
            if (pedidoRelogio) {
                b.setClockResponse(ClockResponse.newBuilder().setTimeMs(ts)); // hora do coordenador agora
            } else {
                b.setElectionResponse(ElectionResponse.newBuilder().setOk(ok));
            }
            Envelope resp = b.build();
            rep.send(resp.toByteArray(), 0);
            log(who, "ENVIADO", resp);
        }
    }

    // Thread: assina o tópico "servers" (proxy XPUB) e guarda o coordenador anunciado.
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
                // A inscrição casa por prefixo: só aceitamos o tópico exato.
                if (!"servers".equals(new String(topico, StandardCharsets.UTF_8))) continue;
                Envelope e;
                try {
                    e = Envelope.parseFrom(corpo);
                } catch (Exception ex) {
                    continue;
                }
                if (!e.hasCoordinatorAnnouncement()) continue;
                atualizarRelogioLogico(e.getLogicalClock());
                log(who, "RECEBIDO", e);
                String coord = e.getCoordinatorAnnouncement().getName();
                if (coord.equals(who)) continue;
                String atual = cl.getCoordenador();
                int rc = cl.rankDe(coord);
                // Alguém de rank menor se anunciou enquanto eu sou o coordenador: contesto (volta a haver eleição).
                if (atual.equals(who) && rc >= 0 && rc < cl.rank) {
                    cl.eleicaoPedida.set(true);
                    continue;
                }
                cl.setCoordenador(coord);
                if (!coord.equals(atual)) {
                    System.out.println("[" + who + "] coordenador atual: " + coord);
                }
            }
        }
    }

    // Lógica do servidor na Parte 3 e 4 (usada só pela thread principal)
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

        // REQ: nome -> REP: rank (isso também cadastra o servidor na referência)
        boolean cadastrar() {
            Integer r = ref.registrar();
            if (r == null) return false;
            registrado = true;
            rank = r;
            cl.rank = r;
            System.out.println("[" + name + "] rank recebido da referência: " + r);
            return true;
        }

        boolean atualizarPares() {
            List<Par> lista = ref.listar();
            if (lista == null) return false;
            cl.setPares(lista);
            return true;
        }

        // Publica no tópico "servers" que este servidor é o coordenador.
        void anunciar() {
            Envelope e = carimbar(Envelope.newBuilder().setCoordinatorAnnouncement(
                    CoordinatorAnnouncement.newBuilder().setName(name)), name);
            pub.sendMore("servers");
            pub.send(e.toByteArray(), 0);
            log(name, "PUBLICADO", e);
        }

        // Bully: pede eleição aos servidores de rank maior; se nenhum responde OK, este servidor assume.
        void eleicao() {
            atualizarPares();
            List<Par> maiores = cl.maiores(rank);
            System.out.println("[" + name + "] eleição iniciada (rank " + rank + "): consultando "
                    + maiores.size() + " servidor(es) de rank maior");
            if (!maiores.isEmpty() && perguntarMaiores(ctx, name, rank, maiores, 1000)) {
                cl.setCoordenador("");
                cl.esperarAteMs.set(passosMs() + 4000); // alguém de rank maior assume e vai anunciar
                System.out.println("[" + name + "] um servidor de rank maior respondeu OK; aguardando o anúncio do coordenador");
            } else {
                cl.setCoordenador(name);
                System.out.println("[" + name + "] eleito coordenador");
                anunciar();
            }
        }

        // Chamada a cada volta do laço: eleição pedida por outro servidor, ou ainda sem coordenador.
        void manter() {
            if (!registrado) return;
            boolean pedida = cl.eleicaoPedida.getAndSet(false);
            boolean nenhum = cl.getCoordenador().isEmpty() && passosMs() >= cl.esperarAteMs.get();
            if (pedida || nenhum) eleicao();
        }

        // A cada CLOCK_SYNC_EVERY mensagens: pede a hora ao coordenador (Cristian com o RTT medido).
        void sincronizar() {
            if (!registrado) return;
            String coord = cl.getCoordenador();
            if (coord.isEmpty()) return; // manter() cuida da eleição
            if (coord.equals(name)) {
                anunciar(); // reforça o anúncio (quem entrou depois pode não ter recebido o primeiro)
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
                System.out.println("[" + name + "] coordenador " + coord + " não respondeu; iniciando eleição");
                cl.setCoordenador("");
                eleicao();
            }
        }

        // A cada heartbeatCada mensagens de clientes: heartbeat e lista de servidores.
        // A cada sincCada: sincronização com o coordenador.
        void depoisDeMensagemDeCliente() {
            if (++desdeSinc >= sincCada) {
                desdeSinc = 0;
                sincronizar();
            }
            if (++desdeHeartbeat < heartbeatCada) return;
            desdeHeartbeat = 0;
            boolean referenciaOk = false; // só pede a lista se a referência respondeu agora
            if (!registrado) {
                referenciaOk = cadastrar();
            } else {
                int hb = ref.heartbeat();
                if (hb == 1) {
                    referenciaOk = true;
                } else if (hb == -1) {
                    System.out.println("[" + name + "] a referência não conhece este servidor; cadastrando de novo");
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
            System.out.println("[" + who + "] sincronização descartada: ida e volta de "
                    + String.format(Locale.ROOT, "%.1f", rttMs) + " ms é grande demais para uma boa estimativa");
            return;
        }
        long estimada = horaRemota + Math.round(rttMs / 2.0);
        long novoOffset = estimada - localAoReceber;
        long antigo = offsetMs.getAndSet(novoOffset);
        System.out.println("[" + who + "] relógio sincronizado com " + de + ": deslocamento " + antigo
                + " ms -> " + novoOffset + " ms (ida e volta: " + String.format(Locale.ROOT, "%.1f", rttMs) + " ms)");
    }

    // REQ com timeout para o serviço de referência. Se ela não responder, o socket REQ
    // fica travado, então ele é recriado (padrão "lazy pirate"). Usado só pela thread principal.
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

        // REQ: nome -> REP: rank. Retorna null se a referência não respondeu.
        Integer registrar() {
            Envelope r = call(Envelope.newBuilder().setRankRequest(RankRequest.newBuilder().setName(who)));
            return (r != null && r.hasRankResponse()) ? Integer.valueOf(r.getRankResponse().getRank()) : null;
        }

        // REQ: heartbeat -> REP: OK. Retorna 1 = ok, 0 = sem resposta válida,
        // -1 = a referência não conhece este servidor (success=false).
        // (Parte 4: a hora não vem mais na resposta do heartbeat.)
        int heartbeat() {
            Envelope r = call(Envelope.newBuilder().setHeartbeatRequest(HeartbeatRequest.newBuilder().setName(who)));
            if (r == null || !r.hasHeartbeatResponse()) return 0;
            return r.getHeartbeatResponse().getSuccess() ? 1 : -1;
        }

        // REQ: list -> REP: [nomes e ranks]. Retorna null se a referência não respondeu.
        List<Par> listar() {
            Envelope r = call(Envelope.newBuilder().setListServersRequest(ListServersRequest.newBuilder()));
            if (r == null || !r.hasListServersResponse()) return null;
            List<Par> out = new ArrayList<>();
            for (ServerInfo s : r.getListServersResponse().getServersList()) {
                out.add(new Par(s.getName(), s.getRank()));
            }
            return out;
        }

        // Cria o socket REQ. Se o endereço não puder ser resolvido (ex.: o serviço de referência
        // ainda não existe/subiu), não derruba o servidor: sock fica null e a próxima chamada tenta de novo.
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
                System.out.println("[" + who + "] não foi possível conectar à referência em "
                        + endpoint + ": " + e.getMessage());
                context.destroySocket(novo);
            }
        }

        private Envelope call(Envelope.Builder builder) {
            if (sock == null) {
                reset();
                if (sock == null) return null;
            }
            Envelope req = carimbar(builder, who);
            log(who, "ENVIADO", req);
            if (!sock.send(req.toByteArray(), 0)) {
                reset();
                return null;
            }
            byte[] bytes = sock.recv(0);
            if (bytes == null) {
                System.out.println("[" + who + "] referência sem resposta em " + endpoint + ", recriando socket");
                reset();
                return null;
            }
            try {
                Envelope rep = Envelope.parseFrom(bytes);
                atualizarRelogioLogico(rep.getLogicalClock());
                log(who, "RECEBIDO", rep);
                return rep;
            } catch (Exception e) {
                reset();
                return null;
            }
        }
    }

    private static void carregarCanais() throws IOException {
        Path path = Paths.get(dataDir, "channels.txt");
        if (Files.exists(path)) {
            for (String linha : Files.readAllLines(path)) {
                if (!linha.isEmpty()) canais.add(linha);
            }
        }
    }

    private static void guardarLogin(String user, long ts) throws IOException {
        Files.writeString(Paths.get(dataDir, "logins.tsv"), ts + "\t" + user + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static void guardarCanal(String canal) throws IOException {
        Files.writeString(Paths.get(dataDir, "channels.txt"), canal + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // 4 bytes (big-endian) com o tamanho + Envelope serializado
    private static void guardarPublicacao(Envelope pubMsg) throws IOException {
        byte[] payload = pubMsg.toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(4 + payload.length);
        buf.putInt(payload.length);
        buf.put(payload);
        Files.write(Paths.get(dataDir, "publications.bin"), buf.array(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // Lê publications.bin e imprime o conteúdo de cada registro.
    private static int dump(String path) throws IOException {
        if (!Files.exists(Paths.get(path))) {
            System.err.println("não foi possível abrir " + path);
            return 1;
        }
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(Paths.get(path)));
        int count = 0;
        while (buf.remaining() >= 4) {
            int n = buf.getInt();
            if (n < 0 || n > buf.remaining()) {
                System.err.println("registro truncado no final do arquivo");
                return 1;
            }
            byte[] rec = new byte[n];
            buf.get(rec);
            log("dump", "ARMAZENADO", Envelope.parseFrom(rec));
            count++;
        }
        System.out.println(count + " publicação(ões) no arquivo");
        return 0;
    }
}