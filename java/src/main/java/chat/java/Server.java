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
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

// Servidor (REP + PUB): trata login, canais e publicações.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv        -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt      -> um canal por linha
//   publications.bin  -> cada publicação: 4 bytes (big-endian) com o tamanho + Envelope serializado
//                        (mesmo formato dos servidores C++ e Node)
// Parte 3:
//   - relógio lógico (contador): incrementado antes de cada envio e enviado no Envelope;
//     ao receber, vira o máximo entre o valor local e o recebido.
//   - serviço de referência (REQ/REP, REFERENCE_ADDR): o servidor pede seu rank ao iniciar e,
//     a cada 10 mensagens de clientes, envia um heartbeat que também pede a hora correta
//     (o relógio físico do servidor é ajustado por um deslocamento, método de Cristian).
// Para listar publications.bin:  java -cp app.jar chat.java.Server --dump /data/publications.bin
public class Server {
    private static final Pattern NOME_VALIDO = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    private static final int MAX_MENSAGEM = 1000; // bytes

    private static Set<String> canais = new HashSet<>();
    private static String dataDir;

    private static final int HEARTBEAT_A_CADA = 10; // mensagens de clientes

    // Relógio lógico (o servidor é single-thread no laço principal)
    private static long relogioLogico = 0;
    // Deslocamento do relógio físico em relação à referência (ms)
    private static long offsetMs = 0;

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--dump")) {
            System.exit(dump(args[1]));
        }

        String name = System.getenv().getOrDefault("SERVER_NAME", "server_java");
        String backend = System.getenv().getOrDefault("BROKER_BACKEND", "tcp://broker:5556");
        String proxyXsub = System.getenv().getOrDefault("PROXY_XSUB", "tcp://proxy:5557");
        String refAddr = System.getenv().getOrDefault("REFERENCE_ADDR", "tcp://reference:5559");
        dataDir = System.getenv().getOrDefault("DATA_DIR", "/data");

        Files.createDirectories(Paths.get(dataDir));
        carregarCanais();

        try (ZContext context = new ZContext()) {
            // Primeiro o servidor se registra na referência (rank). Só depois conecta ao broker,
            // para o broker não mandar requisições de bots a um servidor que ainda não está pronto.
            // REQ: serviço de referência (rank, heartbeat e hora)
            Referencia ref = new Referencia(context, refAddr, name);
            Integer rank = null;
            while (rank == null) {
                Envelope r = ref.call(Envelope.newBuilder()
                        .setRankRequest(RankRequest.newBuilder().setName(name)));
                if (r != null && r.hasRankResponse()) {
                    rank = r.getRankResponse().getRank();
                } else {
                    System.out.println("[" + name + "] sem resposta da referência, tentando novamente em 2s");
                    Thread.sleep(2000);
                }
            }
            System.out.println("[" + name + "] rank recebido da referência: " + rank);

            long msgsClientes = 0;

            // REP: requisições dos bots (via broker)
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.connect(backend);
            // PUB: publicações nos canais (via proxy)
            ZMQ.Socket pubSocket = context.createSocket(SocketType.PUB);
            pubSocket.connect(proxyXsub);
            System.out.println("[" + name + "] conectado ao broker em " + backend + " e ao proxy Pub/Sub em "
                    + proxyXsub + " | dados em " + dataDir + " | canais carregados: " + canais.size());


            while (!Thread.currentThread().isInterrupted()) {
                byte[] reqBytes = socket.recv(0);
                if (reqBytes == null) continue;
                msgsClientes++;

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
                            // NÃO exigimos que o canal exista aqui: basta o nome ser válido.
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
                                // O que vai para o tópico é um Envelope com Publication (igual C++ e Node)
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

                // Heartbeat a cada 10 mensagens de clientes (depois de responder ao bot)
                if (msgsClientes % HEARTBEAT_A_CADA == 0) {
                    heartbeat(ref, name);
                }
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

    // Texto legível do conteúdo da mensagem (para os logs de envio/recebimento)
    private static String flat(Envelope e) {
        return String.valueOf(e).replace('\n', ' ').trim();
    }

    private static void log(String who, String dir, Envelope e) {
        System.out.println(Instant.ofEpochMilli(e.getTimestampMs()) + " | " + who + " | " + dir
                + " | de=" + e.getSender() + " | relogio=" + e.getLogicalClock() + " | " + flat(e));
    }

    // ---- Relógio físico (corrigido pela referência) e relógio lógico ----

    // Hora atual do servidor já corrigida pelo deslocamento obtido da referência
    private static long agora() {
        return System.currentTimeMillis() + offsetMs;
    }

    // Regra 1: incrementa antes de enviar e devolve o valor para ir na mensagem
    private static long proximoRelogioLogico() {
        return ++relogioLogico;
    }

    // Regra 2: ao receber, usa o máximo entre o valor local e o recebido
    private static void atualizarRelogioLogico(long recebido) {
        relogioLogico = Math.max(relogioLogico, recebido);
    }

    // Heartbeat: avisa a referência que o servidor está vivo e pede a hora correta.
    // Ajuste de Cristian: hora da referência + metade do tempo de ida e volta.
    private static void heartbeat(Referencia ref, String name) {
        long t0 = System.currentTimeMillis();
        Envelope rep = ref.call(Envelope.newBuilder()
                .setHeartbeatRequest(HeartbeatRequest.newBuilder().setName(name)));
        long t1 = System.currentTimeMillis();
        if (rep == null || !rep.hasHeartbeatResponse()) {
            System.out.println("[" + name + "] heartbeat sem resposta da referência");
            return;
        }
        HeartbeatResponse h = rep.getHeartbeatResponse();
        if (!h.getSuccess()) {
            System.out.println("[" + name + "] heartbeat não confirmado pela referência");
        }
        if (h.getTimeMs() > 0) {
            long horaEstimada = h.getTimeMs() + (t1 - t0) / 2;
            long novoOffset = horaEstimada - t1;
            System.out.println("[" + name + "] relógio sincronizado: deslocamento " + offsetMs
                    + " ms -> " + novoOffset + " ms (ida e volta: " + (t1 - t0) + " ms)");
            offsetMs = novoOffset;
        }
    }

    // REQ com timeout para o serviço de referência. Se ela não responder, o socket REQ
    // fica travado, então ele é recriado (padrão "lazy pirate").
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

        Envelope call(Envelope.Builder builder) {
            if (sock == null) {
                reset();
                if (sock == null) return null;
            }
            Envelope req = builder.setSender(who)
                    .setTimestampMs(agora())
                    .setLogicalClock(proximoRelogioLogico())
                    .build();
            log(who, "ENVIADO", req);
            if (!sock.send(req.toByteArray(), 0)) {
                reset();
                return null;
            }
            byte[] bytes = sock.recv(0);
            if (bytes == null) {
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