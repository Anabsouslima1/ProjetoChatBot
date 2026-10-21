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
        dataDir = System.getenv().getOrDefault("DATA_DIR", "/data");

        Files.createDirectories(Paths.get(dataDir));
        carregarCanais();

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.connect(backend);
            System.out.println("[" + name + "] Conectado ao broker em " + backend);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // Recebe os bytes (bloqueia até chegar uma mensagem do broker)
                    byte[] reqBytes = socket.recv(0);
                    if (reqBytes == null) continue;
                    
                    System.out.println("[" + name + "] Recebeu " + reqBytes.length + " bytes do broker.");

                    // Converte os bytes binários para o objeto Protobuf
                    Envelope req = Envelope.parseFrom(reqBytes);
                    Envelope.Builder repBuilder = Envelope.newBuilder().setSender(name);

                    System.out.println("[" + name + "] RECEBIDO | de=" + req.getSender() + " | " + req.getBodyCase());

                    // Trata a mensagem de acordo com a ação solicitada
                    switch (req.getBodyCase()) {
                        case LOGIN_REQUEST:
                            String user = req.getLoginRequest().getUsername();
                            // Validação simples do nome de utilizador
                            if (user.isEmpty() || user.length() > 32) {
                                repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                        .setSuccess(false)
                                        .setError(ErrorCode.ERROR_INVALID_USERNAME)
                                        .setErrorMessage("Nome inválido"));
                            } else {
                                salvarLogin(user, req.getTimestampMs());
                                repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                        .setSuccess(true).setError(ErrorCode.ERROR_NONE));
                            }
                            break;

                        case CREATE_CHANNEL_REQUEST:
                            String canal = req.getCreateChannelRequest().getChannelName();
                            if (canais.contains(canal)) {
                                repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                        .setSuccess(false).setError(ErrorCode.ERROR_CHANNEL_EXISTS));
                            } else {
                                canais.add(canal);
                                salvarCanal(canal);
                                repBuilder.setCreateChannelResponse(CreateChannelResponse.newBuilder()
                                        .setSuccess(true).setError(ErrorCode.ERROR_NONE));
                            }
                            break;

                        case LIST_CHANNELS_REQUEST:
                            repBuilder.setListChannelsResponse(ListChannelsResponse.newBuilder()
                                    .addAllChannels(canais));
                            break;

                        default:
                            repBuilder.setLoginResponse(LoginResponse.newBuilder()
                                    .setSuccess(false).setError(ErrorCode.ERROR_UNKNOWN_REQUEST));
                    }

                    // Adiciona o timestamp obrigatório e envia a resposta
                    repBuilder.setTimestampMs(System.currentTimeMillis());
                    socket.send(repBuilder.build().toByteArray(), 0);
                    System.out.println("[" + name + "] ENVIADO a resposta para " + req.getSender());

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

    private static void salvarLogin(String user, long ts) throws IOException {
        Files.writeString(Paths.get(dataDir, "logins.tsv"), ts + "\t" + user + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static void salvarCanal(String canal) throws IOException {
        Files.writeString(Paths.get(dataDir, "channels.txt"), canal + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}