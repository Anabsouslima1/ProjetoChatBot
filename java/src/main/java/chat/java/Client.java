package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import chat.proto.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Client {
    private static final List<String> canaisInscritos = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        String name = System.getenv().getOrDefault("BOT_NAME", "bot_java");
        String endpointReq = System.getenv().getOrDefault("BROKER_FRONTEND", "tcp://broker:5555");
        String endpointPubSub = System.getenv().getOrDefault("PROXY_XPUB", "tcp://127.0.0.1:5558");

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socketReq = conectarReq(context, endpointReq);

            // ==========================================================
            // PARTE 2: Socket SUB (Thread separada para ouvir mensagens)
            // ==========================================================
            ZMQ.Socket socketSub = context.createSocket(SocketType.SUB);
            socketSub.connect(endpointPubSub);
            
            Thread listenerThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        // Recebe o Frame 1 (Tópico/Canal)
                        String topico = socketSub.recvStr();
                        // Recebe o Frame 2 (Mensagem Protobuf)
                        byte[] msgBytes = socketSub.recv();
                        
                        if (topico != null && msgBytes != null) {
                            ChannelMessage msg = ChannelMessage.parseFrom(msgBytes);
                            long tsRecebimento = System.currentTimeMillis();
                            
                            System.out.printf("[%s] 📩 RECEBIDO no canal '%s' | De: %s | Msg: %s | TS_Pub: %d | TS_Rec: %d%n",
                                    name, msg.getChannelName(), msg.getSender(), msg.getMessageText(), msg.getTimestampMs(), tsRecebimento);
                        }
                    } catch (Exception e) {
                        break; // Sai do loop se o contexto for fechado
                    }
                }
            });
            listenerThread.start();

            // ==========================================================
            // PARTE 1: Loop de Login (Mantido igual)
            // ==========================================================
            while (true) {
                Envelope req = Envelope.newBuilder()
                        .setTimestampMs(System.currentTimeMillis())
                        .setSender(name)
                        .setLoginRequest(LoginRequest.newBuilder().setUsername(name))
                        .build();

                socketReq.send(req.toByteArray(), 0);
                byte[] repBytes = socketReq.recv(0);
                
                if (repBytes != null) {
                    Envelope rep = Envelope.parseFrom(repBytes);
                    if (rep.hasLoginResponse() && rep.getLoginResponse().getSuccess()) {
                        System.out.println(name + " | Login realizado com sucesso!");
                        break;
                    }
                } else {
                    context.destroySocket(socketReq);
                    socketReq = conectarReq(context, endpointReq);
                }
                Thread.sleep(2000);
            }

            // ==========================================================
            // PARTE 2: Lógica de Testes exigida pelo Professor
            // ==========================================================
            Random random = new Random();

            while (true) {
                // 1. Pede a lista de canais atualizados
                List<String> canaisDisponiveis = listarCanais(socketReq, name);
                
                if (canaisDisponiveis != null) {
                    // Regra A: Se existirem menos do que 5 canais, criar um novo
                    if (canaisDisponiveis.size() < 5) {
                        String novoCanal = "canal_aleatorio_" + random.nextInt(1000);
                        criarCanal(socketReq, name, novoCanal);
                        canaisDisponiveis.add(novoCanal);
                    }

                    // Regra B: Se inscrito em menos do que 3 canais, inscrever-se em mais um
                    if (canaisInscritos.size() < 3 && !canaisDisponiveis.isEmpty()) {
                        String canalParaAssinar = canaisDisponiveis.get(random.nextInt(canaisDisponiveis.size()));
                        if (!canaisInscritos.contains(canalParaAssinar)) {
                            // Inscreve no socket SUB usando o nome do canal como filtro
                            socketSub.subscribe(canalParaAssinar.getBytes(ZMQ.CHARSET));
                            canaisInscritos.add(canalParaAssinar);
                            System.out.println(name + " | Inscrito no canal: " + canalParaAssinar);
                        }
                    }

                    // Regra C & D: Escolher um canal e enviar 10 mensagens
                    if (!canaisInscritos.isEmpty()) {
                        String canalAlvo = canaisInscritos.get(random.nextInt(canaisInscritos.size()));
                        
                        for (int i = 1; i <= 10; i++) {
                            String textoMsg = "Mensagem de teste " + i + " enviada por " + name;
                            
                            Envelope reqPub = Envelope.newBuilder()
                                    .setTimestampMs(System.currentTimeMillis())
                                    .setSender(name)
                                    .setPublishRequest(PublishRequest.newBuilder()
                                            .setChannelName(canalAlvo)
                                            .setMessageText(textoMsg))
                                    .build();

                            System.out.println(name + " |PUBLICANDO no canal '" + canalAlvo + "' (Msg " + i + "/10)");
                            socketReq.send(reqPub.toByteArray(), 0);
                            
                            byte[] repBytes = socketReq.recv(0);
                            if (repBytes == null) {
                                context.destroySocket(socketReq);
                                socketReq = conectarReq(context, endpointReq);
                                break; // Quebra o for para tentar recuperar a ligação no próximo while
                            }
                            
                            // Regra E: Intervalo de 1 segundo entre mensagens
                            Thread.sleep(1000); 
                        }
                    }
                }
                
                // Pequena pausa antes de recomeçar o grande loop de testes
                Thread.sleep(2000);
            }
        }
    }

    private static ZMQ.Socket conectarReq(ZContext context, String endpoint) {
        ZMQ.Socket socket = context.createSocket(SocketType.REQ);
        socket.setLinger(0);
        socket.setReceiveTimeOut(5000);
        socket.connect(endpoint);
        return socket;
    }

    // Função auxiliar para listar canais
    private static List<String> listarCanais(ZMQ.Socket socket, String name) throws Exception {
        Envelope req = Envelope.newBuilder()
                .setTimestampMs(System.currentTimeMillis())
                .setSender(name)
                .setListChannelsRequest(ListChannelsRequest.newBuilder())
                .build();

        socket.send(req.toByteArray(), 0);
        byte[] repBytes = socket.recv(0);
        if (repBytes != null) {
            Envelope rep = Envelope.parseFrom(repBytes);
            if (rep.hasListChannelsResponse()) {
                return new ArrayList<>(rep.getListChannelsResponse().getChannelsList());
            }
        }
        return null;
    }

    // Função auxiliar para criar canal
    private static void criarCanal(ZMQ.Socket socket, String name, String canal) throws Exception {
        Envelope req = Envelope.newBuilder()
                .setTimestampMs(System.currentTimeMillis())
                .setSender(name)
                .setCreateChannelRequest(CreateChannelRequest.newBuilder().setChannelName(canal))
                .build();
        socket.send(req.toByteArray(), 0);
        socket.recv(0); // Apenas limpa a resposta do buffer
        System.out.println(name + " |Criou novo canal: " + canal);
    }
}