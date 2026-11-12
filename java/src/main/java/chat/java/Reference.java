package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import chat.proto.*;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

// Serviço de referência MÍNIMO, só para testar os servidores Java (Parte 3).
// Socket REP em REFERENCE_BIND (padrão tcp://*:5559). Atende:
//   RankRequest        -> RankResponse   (rank sequencial a partir de 1; o mesmo nome mantém o mesmo rank)
//   ListServersRequest -> ListServersResponse (nome e rank dos servidores disponíveis)
//   HeartbeatRequest   -> HeartbeatResponse   (success=true; Parte 4: SEM a hora, time_ms = 0)
// Servidores que ficam sem enviar heartbeat por HEARTBEAT_TIMEOUT_MS (padrão 30 s) saem da lista.
// Parte 4: a hora correta agora vem do servidor coordenador (eleito entre os servidores),
// não mais da referência.
// Também mantém um relógio lógico (incrementa antes de enviar, máximo ao receber).
public class Reference {
    private static long relogioLogico = 0;

    private static final Map<String, Integer> ranks = new LinkedHashMap<>();       // todos os nomes já vistos
    private static final Map<String, Long> ultimoSinal = new LinkedHashMap<>();    // servidores disponíveis
    private static int proximoRank = 1;

    public static void main(String[] args) throws Exception {
        String name = System.getenv().getOrDefault("REFERENCE_NAME", "reference");
        String bind = System.getenv().getOrDefault("REFERENCE_BIND", "tcp://*:5559");
        long timeoutMs = longEnv("HEARTBEAT_TIMEOUT_MS", 30000);

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.setReceiveTimeOut(1000); // acorda a cada 1 s para remover servidores inativos
            socket.bind(bind);
            System.out.println("[" + name + "] referência ouvindo em " + bind
                    + " | timeout do heartbeat: " + timeoutMs + " ms");

            while (!Thread.currentThread().isInterrupted()) {
                byte[] bytes = socket.recv(0);
                removerInativos(name, timeoutMs);
                if (bytes == null) continue;

                Envelope.Builder rep = Envelope.newBuilder().setSender(name);
                try {
                    Envelope req = Envelope.parseFrom(bytes);
                    relogioLogico = Math.max(relogioLogico, req.getLogicalClock());
                    log(name, "RECEBIDO", req);

                    switch (req.getBodyCase()) {
                        case RANK_REQUEST: {
                            int rank = registrar(nomeDe(req, req.getRankRequest().getName()));
                            rep.setRankResponse(RankResponse.newBuilder().setRank(rank));
                            break;
                        }
                        case LIST_SERVERS_REQUEST: {
                            ListServersResponse.Builder lista = ListServersResponse.newBuilder();
                            for (String s : ultimoSinal.keySet()) {
                                lista.addServers(ServerInfo.newBuilder().setName(s).setRank(ranks.get(s)));
                            }
                            rep.setListServersResponse(lista);
                            break;
                        }
                        case HEARTBEAT_REQUEST: {
                            registrar(nomeDe(req, req.getHeartbeatRequest().getName()));
                            // Parte 4: só o "OK"; a hora (time_ms) não é mais preenchida
                            rep.setHeartbeatResponse(HeartbeatResponse.newBuilder().setSuccess(true));
                            break;
                        }
                        default: {
                            rep.setHeartbeatResponse(HeartbeatResponse.newBuilder().setSuccess(false));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[" + name + "] ERRO ao processar mensagem: " + e.getMessage());
                    rep.setHeartbeatResponse(HeartbeatResponse.newBuilder().setSuccess(false));
                }

                rep.setTimestampMs(System.currentTimeMillis()).setLogicalClock(++relogioLogico);
                Envelope resposta = rep.build();
                socket.send(resposta.toByteArray(), 0);
                log(name, "ENVIADO", resposta);
            }
        }
    }

    // Cadastra (ou renova) o servidor, sem repetir nome, e devolve o rank dele
    private static int registrar(String nome) {
        Integer rank = ranks.get(nome);
        if (rank == null) {
            rank = proximoRank++;
            ranks.put(nome, rank);
        }
        ultimoSinal.put(nome, System.currentTimeMillis());
        return rank;
    }

    private static String nomeDe(Envelope req, String nomeNoCorpo) {
        return nomeNoCorpo.isEmpty() ? req.getSender() : nomeNoCorpo;
    }

    private static void removerInativos(String who, long timeoutMs) {
        long agora = System.currentTimeMillis();
        Iterator<Map.Entry<String, Long>> it = ultimoSinal.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> e = it.next();
            if (agora - e.getValue() > timeoutMs) {
                System.out.println("[" + who + "] sem heartbeat de " + e.getKey()
                        + " há mais de " + timeoutMs + " ms: removido da lista de servidores");
                it.remove();
            }
        }
    }

    private static long longEnv(String key, long padrao) {
        try {
            return Long.parseLong(System.getenv().getOrDefault(key, String.valueOf(padrao)));
        } catch (NumberFormatException e) {
            return padrao;
        }
    }

    private static void log(String who, String dir, Envelope e) {
        System.out.println(Instant.ofEpochMilli(e.getTimestampMs()) + " | " + who + " | " + dir
                + " | de=" + e.getSender() + " | relogio=" + e.getLogicalClock() + " | " + Descricao.de(e));
    }
}