package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import chat.proto.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class Server {
    private static Set<String> canais = new HashSet<>();
    private static String dataDir;

    public static void main(String[] args) throws Exception {
        String name = System.getenv().getOrDefault("SERVER_NAME", "server_java");
        String backend = System.getenv().getOrDefault("BROKER_BACKEND", "tcp://broker:5556");
        
        // PARTE 2: Lemos a nova variável de ambiente do Proxy
        String proxyXsub = System.getenv().getOrDefault("PROXY_XSUB", "tcp://127.0.0.1:5557");
        
        dataDir = System.getenv().getOrDefault("DATA_DIR", "/data");

        Files.createDirectories(Paths.get(dataDir));
        carregarCanais();

        try (ZContext context = new ZContext()) {
            // Socket REP (para receber requisições dos clientes)
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.connect(backend);
            System.out.println("[" + name + "] Conectado ao broker (REP) em " + backend);

            // PARTE 2: Socket PUB (para enviar mensagens para o Proxy Pub/Sub)
            ZMQ.Socket pubSocket = context.createSocket(SocketType.PUB);
            pubSocket.connect(proxyXsub);
            System.out.println("[" + name + "] Conectado ao proxy (PUB) em " + proxyXsub);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    byte[] reqBytes = socket.recv(0);
                    if (reqBytes == null) continue;
                    
                    Envelope req = Envelope.parseFrom(reqBytes);
                    Envelope.Builder repBuilder = Envelope.newBuilder().setSender(name);

                    System.out.println("[" + name + "] RECEBIDO | de=" + req.getSender() + " | " + req.getBodyCase());

                    switch (req.getBodyCase()) {
                        case LOGIN_REQUEST:
                            String user = req.getLoginRequest().getUsername();
                            if (user.isEmpty() || user.length() > 32) {
                                repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_INVALID_USERNAME)
                                        .setErrorMessage("Nome inválido"));
                            } else {
                                guardarLogin(user, req.getTimestampMs());
                                repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                        .setSuccess(true).setError(ErrorCode.ERROR_NONE));
                            }
                            break;

                        case CREATE_CHANNEL_REQUEST:
                            String canal = req.getCreateChannelRequest().getChannelName();
                            // Se o canal já existe, retorna erro
                            if (canais.contains(canal)) {
                                repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                        .setSuccess(false).setError(ErrorCode.ERROR_CHANNEL_EXISTS));
                            } else {
                                canais.add(canal);
                                guardarCanal(canal);
                                repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                        .setSuccess(true).setError(ErrorCode.ERROR_NONE));
                            }
                            break;

                        case LIST_CHANNELS_REQUEST:
                            repBuilder.setListChannelsResponse(ListChannelsResponse.newBuilder()
                                    .addAllChannels(canais));
                            break;

                        // ==========================================================
                        // PARTE 2: Novo processamento de publicação de mensagens
                        // ==========================================================
                        case PUBLISH_REQUEST:
                            String canalAlvo = req.getPublishRequest().getChannelName();
                            String textoMensagem = req.getPublishRequest().getMessageText();

                            // 1. Verifica se o canal onde o bot quer publicar realmente existe
                            if (!canais.contains(canalAlvo)) {
                                repBuilder.setPublishResponse(PublishResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_CHANNEL_NOT_FOUND) // Erro genérico caso não exista um erro específico no proto
                                        .setErrorMessage("O canal especificado não existe."));
                            } else {
                                long tsAtual = System.currentTimeMillis();
                                
                                // 2. Monta o objeto ChannelMessage que será enviado no Pub/Sub
                                ChannelMessage msgPubSub = ChannelMessage.newBuilder()
                                        .setTimestampMs(tsAtual)
                                        .setSender(req.getSender())
                                        .setChannelName(canalAlvo)
                                        .setMessageText(textoMensagem)
                                        .build();

                                // 3. Guarda a mensagem no disco (Persistência da Parte 2)
                                guardarMensagem(tsAtual, req.getSender(), canalAlvo, textoMensagem);

                                // 4. Envio Multipart no ZeroMQ:
                                // O Frame 1 é o Tópico (Nome do Canal) em String puro.
                                pubSocket.sendMore(canalAlvo);
                                // O Frame 2 é a mensagem em formato binário Protobuf.
                                pubSocket.send(msgPubSub.toByteArray(), 0);

                                System.out.println("[" + name + "] MENSAGEM PUBLICADA no canal: " + canalAlvo);

                                // 5. Responde ao bot dizendo que a publicação foi um sucesso
                                repBuilder.setPublishResponse(PublishResponse.newBuilder()
                                        .setSuccess(true).setError(ErrorCode.ERROR_NONE));
                            }
                            break;

                        default:
                            repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                    .setSuccess(false).setError(ErrorCode.ERROR_UNKNOWN_REQUEST));
                    }

                    repBuilder.setTimestampMs(System.currentTimeMillis());
                    socket.send(repBuilder.build().toByteArray(), 0);

                } catch (Exception e) {
                    System.err.println("[" + name + "] ERRO ao processar mensagem: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    private static void carregarCanais() throws IOException {
        Path path = Paths.get(dataDir, "channels.txt");
        if (Files.exists(path)) {
            canais.addAll(Files.readAllLines(path));
        }
    }

    private static void guardarLogin(String user, long ts) throws IOException {
        Files.writeString(Paths.get(dataDir, "logins.tsv"), ts + "\t" + user + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static void guardarCanal(String canal) throws IOException {
        Files.writeString(Paths.get(dataDir, "channels.txt"), canal + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // PARTE 2: Função exigida para guardar o histórico das mensagens trocadas
    private static void guardarMensagem(long ts, String sender, String canal, String mensagem) throws IOException {
        String linha = String.format("[%d] %s no canal '%s': %s%n", ts, sender, canal, mensagem);
        Files.writeString(Paths.get(dataDir, "historico_mensagens.txt"), linha, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
