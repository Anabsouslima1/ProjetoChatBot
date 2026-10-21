package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import chat.proto.Chat.*;

public class Client {
    public static void main(String[] args) throws Exception {
        String name = System.getenv().getOrDefault("BOT_NAME", "bot_java");
        String endpoint = System.getenv().getOrDefault("BROKER_FRONTEND", "tcp://broker:5555");
        String ownChannel = System.getenv().getOrDefault("BOT_CHANNEL", "canal-" + name);

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect(endpoint);

            // 1. Loop de Login
            while (true) {
                Envelope req = Envelope.newBuilder()
                        .setTimestampMs(System.currentTimeMillis())
                        .setSender(name)
                        .setLoginRequest(LoginRequest.newBuilder().setUsername(name))
                        .build();

                socket.send(req.toByteArray(), 0);
                Envelope rep = Envelope.parseFrom(socket.recv(0));

                if (rep.hasLoginResponse() && rep.getLoginResponse().getSuccess()) {
                    System.out.println(name + " | Login realizado com sucesso");
                    break;
                }
                System.out.println(name + " | Falha no login, tentando novamente...");
                Thread.sleep(2000);
            }

            // 2. Listar Canais e Criar
            while (true) {
                Envelope reqList = Envelope.newBuilder()
                        .setTimestampMs(System.currentTimeMillis())
                        .setSender(name)
                        .setListChannelsRequest(ListChannelsRequest.newBuilder())
                        .build();

                socket.send(reqList.toByteArray(), 0);
                Envelope repList = Envelope.parseFrom(socket.recv(0));

                if (repList.hasListChannelsResponse()) {
                    java.util.List<String> canais = repList.getListChannelsResponse().getChannelsList();
                    if (!canais.contains(ownChannel)) {
                        Envelope reqCreate = Envelope.newBuilder()
                                .setTimestampMs(System.currentTimeMillis())
                                .setSender(name)
                                .setCreateChannelRequest(CreateChannelRequest.newBuilder().setChannelName(ownChannel))
                                .build();
                        
                        socket.send(reqCreate.toByteArray(), 0);
                        socket.recv(0); // Aguarda resposta
                        System.out.println(name + " | Pedido de criação do canal " + ownChannel + " enviado.");
                    }
                }
                Thread.sleep(5000);
            }
        }
    }
}