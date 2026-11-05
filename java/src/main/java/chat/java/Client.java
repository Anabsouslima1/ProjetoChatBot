package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import chat.proto.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

// Cliente (bot). Ao conectar: login -> loop infinito de rodadas. Em cada rodada o bot:
//   1. lista os canais; se houver menos de 5, cria um novo;
//   2. se estiver inscrito em menos de 3 canais, se inscreve em mais um (escolhido ao acaso);
//   3. escolhe um canal da lista e publica 10 mensagens aleatórias com 1 s de intervalo.
// Parte 3 - relógio lógico: um contador que começa em 0, é incrementado antes de cada envio
// (e vai no Envelope) e, ao receber qualquer mensagem, vira o máximo entre o valor local e o recebido.
// Uma thread própria recebe (SUB) e exibe tudo que chega nos canais em que o bot está inscrito.
public class Client {
    private static final String[] FRASES = {
        "ola pessoal!", "alguem por ai?", "testando o canal", "mensagem aleatoria",
        "sistemas distribuidos", "zeromq e protobuf", "bom dia, canal!", "publicando de novo",
        "ping", "tudo certo por aqui"
    };

    // Relógio lógico do bot. É usado pela thread principal (REQ) e pela thread do SUB,
    // por isso é atômico.
    private static final AtomicLong relogioLogico = new AtomicLong(0);

    // Regra 1: incrementa antes de enviar e devolve o valor para ir na mensagem
    private static long proximoRelogio() {
        return relogioLogico.incrementAndGet();
    }

    // Regra 2: ao receber, usa o máximo entre o valor local e o recebido
    private static void atualizarRelogio(long recebido) {
        relogioLogico.accumulateAndGet(recebido, Math::max);
    }

    public static void main(String[] args) throws Exception {
        String name = System.getenv().getOrDefault("BOT_NAME", "bot_java");
        String endpointReq = System.getenv().getOrDefault("BROKER_FRONTEND", "tcp://broker:5555");
        String endpointPubSub = System.getenv().getOrDefault("PROXY_XPUB", "tcp://proxy:5558");
        int msgsPorRodada = intEnv("MSGS_PER_ROUND", 10);
        int intervaloMs = intEnv("MSG_INTERVAL_MS", 1000);

        Random random = new Random();

        try (ZContext context = new ZContext()) {
            Rpc rpc = new Rpc(context, endpointReq, name);

            // O socket SUB é criado aqui, mas usado só pela thread do assinante
            Assinante assinante = new Assinante(context.createSocket(SocketType.SUB), endpointPubSub, name);
            Thread listener = new Thread(assinante, "sub-" + name);
            listener.setDaemon(true);
            listener.start();

            // Login (repetido até dar certo)
            while (true) {
                Envelope rep = rpc.call(base(name).setLoginRequest(LoginRequest.newBuilder().setUsername(name)));
                if (rep != null && rep.hasLoginResponse() && rep.getLoginResponse().getSuccess()) {
                    System.out.println(name + " | login realizado com sucesso");
                    break;
                }
                System.out.println(name + " | login falhou, tentando novamente em 2s");
                Thread.sleep(2000);
            }

            while (true) {
                // Lista os canais disponíveis
                Envelope lrep = rpc.call(base(name).setListChannelsRequest(ListChannelsRequest.newBuilder()));
                if (lrep == null || !lrep.hasListChannelsResponse()) {
                    Thread.sleep(2000);
                    continue;
                }
                List<String> canais = new ArrayList<>(lrep.getListChannelsResponse().getChannelsList());

                // 1) menos de 5 canais -> cria um novo
                if (canais.size() < 5) {
                    String novo = "canal-" + name + "-" + (1000 + random.nextInt(9000));
                    if (novo.length() > 32) novo = novo.substring(0, 32);
                    Envelope crep = rpc.call(base(name).setCreateChannelRequest(
                            CreateChannelRequest.newBuilder().setChannelName(novo)));
                    if (crep != null && crep.hasCreateChannelResponse()
                            && crep.getCreateChannelResponse().getSuccess()) {
                        canais.add(novo);
                    }
                }

                // 2) inscrito em menos de 3 canais -> se inscreve em mais um (ao acaso)
                if (assinante.total() < 3) {
                    List<String> candidatos = new ArrayList<>();
                    for (String c : canais) {
                        if (!assinante.assinado(c)) candidatos.add(c);
                    }
                    if (!candidatos.isEmpty()) {
                        String c = candidatos.get(random.nextInt(candidatos.size()));
                        assinante.assinar(c);
                        System.out.println(name + " | inscrito no canal " + c
                                + " (total de inscrições: " + assinante.total() + ")");
                        // A inscrição leva um instante para chegar aos publicadores
                        // (ZeroMQ "slow joiner"): esperamos um pouco para não perder as primeiras mensagens.
                        Thread.sleep(300);
                    }
                }

                // 3) escolhe um canal da lista e publica N mensagens, 1 por segundo
                if (canais.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }
                String alvo = canais.get(random.nextInt(canais.size()));
                for (int i = 1; i <= msgsPorRodada; i++) {
                    String texto = FRASES[random.nextInt(FRASES.length)] + " (#" + i + " de " + name + ")";
                    rpc.call(base(name).setPublishRequest(PublishRequest.newBuilder()
                            .setChannel(alvo)
                            .setMessage(texto)));
                    Thread.sleep(intervaloMs);
                }
            }
        }
    }

    private static int intEnv(String key, int padrao) {
        try {
            return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(padrao)));
        } catch (NumberFormatException e) {
            return padrao;
        }
    }

    private static Envelope.Builder base(String name) {
        return Envelope.newBuilder().setTimestampMs(System.currentTimeMillis()).setSender(name);
    }

    // Texto legível do conteúdo da mensagem (para os logs de envio/recebimento)
    private static String flat(Envelope e) {
        return String.valueOf(e).replace('\n', ' ').trim();
    }

    private static void log(String who, String dir, Envelope e) {
        System.out.println(Instant.ofEpochMilli(e.getTimestampMs()) + " | " + who + " | " + dir
                + " | de=" + e.getSender() + " | relogio=" + e.getLogicalClock() + " | " + flat(e));
    }

    // REQ com timeout: se o servidor não responder, o socket REQ fica travado,
    // então ele é recriado (padrão "lazy pirate").
    private static class Rpc {
        private final ZContext context;
        private final String endpoint;
        private final String who;
        private ZMQ.Socket sock;

        Rpc(ZContext context, String endpoint, String who) {
            this.context = context;
            this.endpoint = endpoint;
            this.who = who;
            reset();
        }

        private void reset() {
            if (sock != null) context.destroySocket(sock);
            sock = context.createSocket(SocketType.REQ);
            sock.setLinger(0);
            sock.setReceiveTimeOut(5000);
            sock.connect(endpoint);
        }

        Envelope call(Envelope.Builder builder) {
            // O contador é incrementado imediatamente antes do envio
            Envelope req = builder.setLogicalClock(proximoRelogio()).build();
            log(who, "ENVIADO", req);
            sock.send(req.toByteArray(), 0);
            byte[] bytes = sock.recv(0);
            if (bytes == null) {
                System.out.println(who + " | sem resposta válida, recriando socket");
                reset();
                return null;
            }
            try {
                Envelope rep = Envelope.parseFrom(bytes);
                atualizarRelogio(rep.getLogicalClock());
                log(who, "RECEBIDO", rep);
                return rep;
            } catch (Exception e) {
                System.out.println(who + " | resposta inválida, recriando socket");
                reset();
                return null;
            }
        }
    }

    // Assinante (SUB) em thread própria. Um socket ZeroMQ não pode ser usado por duas threads,
    // então a thread principal só registra o canal desejado; a inscrição de fato é feita aqui.
    private static class Assinante implements Runnable {
        private final ZMQ.Socket sub;
        private final String endpoint;
        private final String who;
        private final Set<String> inscritos = ConcurrentHashMap.newKeySet();
        private final Queue<String> pendentes = new ConcurrentLinkedQueue<>();

        Assinante(ZMQ.Socket sub, String endpoint, String who) {
            this.sub = sub;
            this.endpoint = endpoint;
            this.who = who;
        }

        void assinar(String canal) {
            if (inscritos.add(canal)) pendentes.add(canal);
        }

        int total() {
            return inscritos.size();
        }

        boolean assinado(String canal) {
            return inscritos.contains(canal);
        }

        @Override
        public void run() {
            try {
                sub.setReceiveTimeOut(200);
                sub.setLinger(0);
                sub.connect(endpoint);

                while (!Thread.currentThread().isInterrupted()) {
                    String c;
                    while ((c = pendentes.poll()) != null) {
                        sub.subscribe(c.getBytes(StandardCharsets.UTF_8));
                    }

                    // Mensagem do Pub/Sub: frame 1 = tópico (canal), frame 2 = Envelope
                    String topico = sub.recvStr();
                    if (topico == null) continue;
                    if (!sub.hasReceiveMore()) continue;
                    byte[] corpo = sub.recv();
                    if (corpo == null) continue;

                    long recebidoEm = System.currentTimeMillis();
                    // A inscrição do ZeroMQ casa por prefixo ("geral" também recebe "geral2"):
                    // só exibimos canais em que o bot realmente se inscreveu.
                    if (!inscritos.contains(topico)) continue;

                    try {
                        Envelope e = Envelope.parseFrom(corpo);
                        atualizarRelogio(e.getLogicalClock());
                        if (!e.hasPublication()) continue;
                        System.out.println(Instant.ofEpochMilli(recebidoEm) + " | " + who
                                + " | MENSAGEM | canal=" + topico
                                + " | relogio_msg=" + e.getLogicalClock()
                                + " | relogio_local=" + relogioLogico.get()
                                + " | de=" + e.getPublication().getAuthor()
                                + " | enviada_em=" + Instant.ofEpochMilli(e.getTimestampMs())
                                + " | recebida_em=" + Instant.ofEpochMilli(recebidoEm)
                                + " | \"" + e.getPublication().getMessage() + "\"");
                    } catch (Exception ignorada) {
                        // mensagem inválida: ignora
                    }
                }
            } catch (Exception e) {
                // contexto fechado: encerra a thread
            }
        }
    }
}