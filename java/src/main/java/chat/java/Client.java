package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import chat.proto.*;

import java.util.List;

public class Client {
    public static void main(String[] args) throws Exception {
        String name = System.getenv().getOrDefault("BOT_NAME", "bot_java");
        String endpoint = System.getenv().getOrDefault("BROKER_FRONTEND", "tcp://broker:5555");
        String ownChannel = System.getenv().getOrDefault("BOT_CHANNEL", "canal-" + name);

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = conectar(context, endpoint);

            // 1. Loop de Login
            while (true) {
                Envelope req = Envelope.newBuilder()
                        .setTimestampMs(System.currentTimeMillis())
                        .setSender(name)
                        .setLoginRequest(LoginRequest.newBuilder().setUsername(name))
                        .build();

                System.out.println(name + " | ENVIADO | de=" + name + " | login_request");
                socket.send(req.toByteArray(), 0);
                
                byte[] repBytes = socket.recv(0);
                if (repBytes == null) {
                    System.out.println(name + " | sem resposta válida, recriando socket...");
                    context.destroySocket(socket);
                    socket = conectar(context, endpoint);
                    Thread.sleep(2000);
                    continue;
                }

                Envelope rep = Envelope.parseFrom(repBytes);
                if (rep.hasLoginResponse() && rep.getLoginResponse().getSuccess()) {
                    System.out.println(name + " | login realizado com sucesso");
                    break;
                }
                System.out.println(name + " | login falhou, tentando novamente em 2s");
                Thread.sleep(2000);
            }

            // 2. Listar Canais e Criar os que faltam
            String[] canaisDesejados = {"geral", ownChannel};
            
            while (true) {
                Envelope reqList = Envelope.newBuilder()
                        .setTimestampMs(System.currentTimeMillis())
                        .setSender(name)
                        .setListChannelsRequest(ListChannelsRequest.newBuilder())
                        .build();

                System.out.println(name + " | ENVIADO | de=" + name + " | list_channels_request");
                socket.send(reqList.toByteArray(), 0);
                
                byte[] repBytes = socket.recv(0);
                if (repBytes == null) {
                    System.out.println(name + " | sem resposta válida (list_channels), recriando socket...");
                    context.destroySocket(socket);
                    socket = conectar(context, endpoint);
                    Thread.sleep(2000);
                    continue;
                }

                Envelope repList = Envelope.parseFrom(repBytes);
                if (repList.hasListChannelsResponse()) {
                    List<String> canaisExistentes = repList.getListChannelsResponse().getChannelsList();
                    
                    for (String canalDesejado : canaisDesejados) {
                        if (!canaisExistentes.contains(canalDesejado)) {
                            Envelope reqCreate = Envelope.newBuilder()
                                    .setTimestampMs(System.currentTimeMillis())
                                    .setSender(name)
                                    .setCreateChannelRequest(CreateChannelRequest.newBuilder().setChannelName(canalDesejado))
                                    .build();
                            
                            System.out.println(name + " | ENVIADO | de=" + name + " | create_channel_request{channel_name=" + canalDesejado + "}");
                            socket.send(reqCreate.toByteArray(), 0);
                            
                            byte[] createRepBytes = socket.recv(0);
                            if (createRepBytes == null) {
                                 System.out.println(name + " | sem resposta válida ao criar canal, recriando socket...");
                                 context.destroySocket(socket);
                                 socket = conectar(context, endpoint);
                                 break; // Recria e tenta listar/criar de novo no próximo loop
                            } else {
                                 Envelope createRep = Envelope.parseFrom(createRepBytes);
                                 boolean sucesso = createRep.getCreateChannelResponse().getSuccess();
                                 System.out.println(name + " | RECEBIDO | de=" + createRep.getSender() + " | create_channel_response{success=" + sucesso + "}");
                            }
                        }
                    }
                }
                
                // Repete o processo a cada 5 segundos para espelhar o comportamento do bot C++
                Thread.sleep(5000); 
            }
        }
    }

    // Função auxiliar para criar o socket com Timeout (Lazy Pirate Pattern)
    private static ZMQ.Socket conectar(ZContext context, String endpoint) {
        ZMQ.Socket socket = context.createSocket(SocketType.REQ);
        socket.setLinger(0);
        socket.setReceiveTimeOut(5000); // 5 segundos de espera máxima
        socket.connect(endpoint);
        return socket;
    }
}