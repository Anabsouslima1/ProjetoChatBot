package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import chat.proto.*;

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
                    continue; // Tenta fazer login novamente
                }

                Envelope rep = Envelope.parseFrom(repBytes);
                if (rep.hasLoginResponse() && rep.getLoginResponse().getSuccess()) {
                    System.out.println(name + " | login realizado com sucesso");
                    break;
                }
                System.out.println(name + " | login falhou, tentando novamente em 2s");
                Thread.sleep(2000);
            }

            // O código para criar canais (Parte 2) entrará aqui depois que o login funcionar!
            System.out.println(name + " | Bot ativo e logado!");
            while(true) { Thread.sleep(10000); }
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