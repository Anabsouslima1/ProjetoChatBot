package chat.java;

import chat.proto.Envelope;
import chat.proto.Publication;
import chat.proto.ReplicaRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

final class Replica {

    static final class Estatisticas {
        final int total;
        final int logins;
        final int canais;
        final int publicacoes;
        final String hash;

        Estatisticas(int total, int logins, int canais, int publicacoes, String hash) {
            this.total = total;
            this.logins = logins;
            this.canais = canais;
            this.publicacoes = publicacoes;
            this.hash = hash;
        }
    }

    static final class Faltantes {
        final List<ReplicaRecord> registros;
        final boolean maisRegistros;

        Faltantes(List<ReplicaRecord> registros, boolean maisRegistros) {
            this.registros = registros;
            this.maisRegistros = maisRegistros;
        }
    }

    private static final class Progresso {
        long contiguo = 0;
        final TreeSet<Long> acima = new TreeSet<>();
    }

    private final Path pasta;
    private final String origem;
    private final TreeMap<String, TreeMap<Long, ReplicaRecord>> todos = new TreeMap<>();
    private final Map<String, Progresso> progresso = new HashMap<>();
    private final TreeSet<String> canais = new TreeSet<>();
    private final ArrayDeque<ReplicaRecord> filaDeSaida = new ArrayDeque<>();
    
    private long proximaSeq = 0;
    private int total = 0;
    private int logins = 0;
    private int registrosDeCanal = 0;
    private int publicacoes = 0;

    Replica(String pasta, String nomeServidor) throws IOException {
        this.pasta = Paths.get(pasta);
        Files.createDirectories(this.pasta);
        this.origem = nomeServidor + "@" + System.currentTimeMillis();
        carregar();
    }

    synchronized boolean registrarLogin(String usuario, long horaMs) {
        ReplicaRecord registro = ReplicaRecord.newBuilder()
                .setKind(ReplicaRecord.Kind.KIND_LOGIN)
                .setName(usuario)
                .setTimestampMs(horaMs)
                .setLogicalClock(Server.relogioLogicoAtual())
                .build();
        return registrarLocal(registro);
    }

    // Retorna: 0 = criado, 1 = já existente, 2 = erro de IO
    synchronized int registrarCanal(String canal) {
        if (canais.contains(canal)) return 1;
        ReplicaRecord registro = ReplicaRecord.newBuilder()
                .setKind(ReplicaRecord.Kind.KIND_CHANNEL)
                .setName(canal)
                .setTimestampMs(Server.agora())
                .setLogicalClock(Server.relogioLogicoAtual())
                .build();
        return registrarLocal(registro) ? 0 : 2;
    }

    synchronized boolean registrarPublicacao(Envelope publicacao) {
        ReplicaRecord registro = ReplicaRecord.newBuilder()
                .setKind(ReplicaRecord.Kind.KIND_PUBLICATION)
                .setName(publicacao.getPublication().getChannel())
                .setText(publicacao.getPublication().getMessage())
                .setAuthor(publicacao.getPublication().getAuthor())
                .setTimestampMs(publicacao.getTimestampMs())
                .setLogicalClock(publicacao.getLogicalClock())
                .build();
        return registrarLocal(registro);
    }

    synchronized List<String> canais() {
        return new ArrayList<>(canais);
    }

    synchronized int aplicarRemotos(List<ReplicaRecord> registros) {
        int novos = 0;
        for (ReplicaRecord registro : registros) {
            if (registro.getOrigin().isEmpty() || registro.getSeq() <= 0 || jaConhece(registro)) continue;
            guardar(registro, true);
            novos++;
        }
        return novos;
    }

    synchronized Map<String, Long> versao() {
        Map<String, Long> versao = new TreeMap<>();
        for (Map.Entry<String, Progresso> e : progresso.entrySet()) {
            versao.put(e.getKey(), e.getValue().contiguo);
        }
        return versao;
    }

    synchronized Faltantes faltantes(Map<String, Long> tem, int maximo) {
        List<ReplicaRecord> saida = new ArrayList<>();
        for (Map.Entry<String, TreeMap<Long, ReplicaRecord>> porOrigem : todos.entrySet()) {
            Long jaTem = tem.get(porOrigem.getKey());
            long apartirDe = jaTem == null ? 0 : jaTem;
            for (ReplicaRecord registro : porOrigem.getValue().tailMap(apartirDe, false).values()) {
                if (saida.size() >= maximo) return new Faltantes(saida, true);
                saida.add(registro);
            }
        }
        return new Faltantes(saida, false);
    }

    synchronized List<ReplicaRecord> esperarFilaDeSaida(long timeoutMs, int maximo) throws InterruptedException {
        if (filaDeSaida.isEmpty()) wait(timeoutMs);
        List<ReplicaRecord> lote = new ArrayList<>();
        while (!filaDeSaida.isEmpty() && lote.size() < maximo) {
            lote.add(filaDeSaida.poll());
        }
        return lote;
    }

    synchronized Estatisticas estatisticas() {
        int hash = (int) 2166136261L; // FNV-1a offset basis
        for (Map.Entry<String, TreeMap<Long, ReplicaRecord>> porOrigem : todos.entrySet()) {
            for (Long seq : porOrigem.getValue().keySet()) {
                byte[] bytes = (porOrigem.getKey() + "#" + seq + ";").getBytes(StandardCharsets.UTF_8);
                for (byte b : bytes) {
                    hash = (hash ^ (b & 0xff)) * 16777619;
                }
            }
        }
        return new Estatisticas(total, logins, registrosDeCanal, publicacoes, String.format("%08x", hash));
    }

    private boolean jaConhece(ReplicaRecord registro) {
        TreeMap<Long, ReplicaRecord> daOrigem = todos.get(registro.getOrigin());
        return daOrigem != null && daOrigem.containsKey(registro.getSeq());
    }

    private boolean registrarLocal(ReplicaRecord semId) {
        ReplicaRecord registro = semId.toBuilder().setOrigin(origem).setSeq(++proximaSeq).build();
        if (!guardar(registro, true)) return false; 
        filaDeSaida.add(registro);
        notifyAll();
        return true;
    }

    private boolean guardar(ReplicaRecord registro, boolean gravarVisoes) {
        boolean gravou = acrescentarComTamanho(pasta.resolve("records.bin"), registro.toByteArray());
        indexar(registro);
        if (gravarVisoes) gravou = gravarVisao(registro) && gravou;
        return gravou;
    }

    private void indexar(ReplicaRecord registro) {
        todos.computeIfAbsent(registro.getOrigin(), o -> new TreeMap<>()).put(registro.getSeq(), registro);

        Progresso p = progresso.computeIfAbsent(registro.getOrigin(), o -> new Progresso());
        if (registro.getSeq() == p.contiguo + 1) {
            p.contiguo++;
            while (p.acima.remove(p.contiguo + 1)) p.contiguo++;
        } else if (registro.getSeq() > p.contiguo + 1) {
            p.acima.add(registro.getSeq());
        }

        total++;
        switch (registro.getKind()) {
            case KIND_LOGIN:
                logins++;
                break;
            case KIND_CHANNEL:
                registrosDeCanal++;
                canais.add(registro.getName());
                break;
            case KIND_PUBLICATION:
                publicacoes++;
                break;
            default:
                break;
        }
    }

    private static boolean acrescentarComTamanho(Path arquivo, byte[] conteudo) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(4 + conteudo.length);
            buffer.putInt(conteudo.length);
            buffer.put(conteudo);
            Files.write(arquivo, buffer.array(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean acrescentarTexto(Path arquivo, String texto) {
        try {
            Files.write(arquivo, texto.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean gravarVisao(ReplicaRecord registro) {
        switch (registro.getKind()) {
            case KIND_LOGIN:
                return acrescentarTexto(pasta.resolve("logins.tsv"),
                        registro.getTimestampMs() + "\t" + registro.getName() + "\n");
            case KIND_CHANNEL:
                return acrescentarTexto(pasta.resolve("channels.txt"), registro.getName() + "\n");
            case KIND_PUBLICATION: {
                int arroba = registro.getOrigin().indexOf('@');
                Envelope publicacao = Envelope.newBuilder()
                        .setTimestampMs(registro.getTimestampMs())
                        .setSender(arroba < 0 ? registro.getOrigin() : registro.getOrigin().substring(0, arroba))
                        .setLogicalClock(registro.getLogicalClock())
                        .setPublication(Publication.newBuilder()
                                .setChannel(registro.getName())
                                .setMessage(registro.getText())
                                .setAuthor(registro.getAuthor()))
                        .build();
                return acrescentarComTamanho(pasta.resolve("publications.bin"), publicacao.toByteArray());
            }
            default:
                return true;
        }
    }

    private void carregar() throws IOException {
        Path arquivo = pasta.resolve("records.bin");
        if (!Files.exists(arquivo)) return;
        ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(arquivo));
        while (buffer.remaining() >= 4) {
            int tamanho = buffer.getInt();
            if (tamanho < 0 || tamanho > buffer.remaining()) break;
            byte[] conteudo = new byte[tamanho];
            buffer.get(conteudo);
            try {
                ReplicaRecord registro = ReplicaRecord.parseFrom(conteudo);
                if (!jaConhece(registro)) indexar(registro);
            } catch (Exception e) {
                // Falhas no parse ou registros corrompidos são ignorados silenciosamente
            }
        }
    }
}