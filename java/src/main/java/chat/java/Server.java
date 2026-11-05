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
// Para listar publications.bin:  java -cp app.jar chat.java.Server --dump /data/publications.bin
public class Server {
    private static final Pattern NOME_VALIDO = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    private static final int MAX_MENSAGEM = 1000; // bytes

    private static Set<String> canais = new HashSet<>();
    private static String dataDir;

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--dump")) {
            System.exit(dump(args[1]));
        }

        String name = System.getenv().getOrDefault("SERVER_NAME", "server_java");
        String backend = System.getenv().getOrDefault("BROKER_BACKEND", "tcp://broker:5556");
        String proxyXsub = System.getenv().getOrDefault("PROXY_XSUB", "tcp://proxy:5557");
        dataDir = System.getenv().getOrDefault("DATA_DIR", "/data");

        Files.createDirectories(Paths.get(dataDir));
        carregarCanais();

        try (ZContext context = new ZContext()) {
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

                Envelope.Builder repBuilder = Envelope.newBuilder().setSender(name);
                try {
                    Envelope req = Envelope.parseFrom(reqBytes);
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
                                guardarLogin(user, System.currentTimeMillis());
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
                                        .setTimestampMs(System.currentTimeMillis())
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
                repBuilder.setTimestampMs(System.currentTimeMillis());
                Envelope rep = repBuilder.build();
                socket.send(rep.toByteArray(), 0);
                log(name, "ENVIADO", rep);
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
                + " | de=" + e.getSender() + " | " + flat(e));
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