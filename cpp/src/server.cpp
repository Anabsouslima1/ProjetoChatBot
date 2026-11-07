// Servidor (REP + PUB + REQ para a referência): login, canais, publicações, relógios e heartbeat.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv        -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt      -> um canal por linha
//   publications.bin  -> cada publicação: 4 bytes (big-endian) com o tamanho + Envelope serializado
//                        (protobuf). Contém canal, autor, texto, timestamp e relógio lógico.
// Para listar o conteúdo de publications.bin:  server --dump /data/publications.bin
//
// Parte 3:
//   - relógio lógico em todas as mensagens (ver common.hpp);
//   - ao subir, pede o rank à referência (isso cadastra o servidor);
//   - a cada HEARTBEAT_EVERY (10) mensagens de clientes recebidas, envia heartbeat à referência.
//
// Parte 4 (a hora deixa de vir da referência):
//   - um servidor é o coordenador (eleição pelo algoritmo do valentão/bully: vence o maior rank);
//   - a cada CLOCK_SYNC_EVERY (15) mensagens de clientes, o servidor pede a hora ao coordenador
//     (REQ clock_request / REP clock_response) e ajusta o relógio físico;
//   - se o coordenador não responde (ou ainda não há um), inicia eleição: pede eleição aos servidores
//     de rank maior (REQ election_request / REP OK); se ninguém de rank maior responde, assume e
//     publica o nome no tópico "servers"; todos assinam esse tópico e guardam o coordenador.
//   - cada servidor atende os pedidos dos outros em tcp://*:(PEER_PORT_BASE + rank). O endereço de um
//     par é tcp://<PEER_HOST ou nome do servidor>:(PEER_PORT_BASE + rank do par).
#include <zmq.hpp>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <sys/stat.h>
#include <thread>
#include <vector>

#include "common.hpp"

class Storage {
 public:
  explicit Storage(const std::string& dir) : dir_(dir) {
    mkdir(dir_.c_str(), 0755);
    std::ifstream in(dir_ + "/channels.txt");
    std::string line;
    while (std::getline(in, line))
      if (!line.empty()) channels_.insert(line);
  }

  bool add_login(const std::string& user, int64_t ts) {
    std::ofstream out(dir_ + "/logins.tsv", std::ios::app);
    out << ts << '\t' << user << '\n';
    out.flush();
    return static_cast<bool>(out);
  }

  // 0 = criado, 1 = já existe, 2 = erro de disco
  int add_channel(const std::string& name) {
    if (channels_.count(name)) return 1;
    std::ofstream out(dir_ + "/channels.txt", std::ios::app);
    out << name << '\n';
    out.flush();
    if (!out) return 2;
    channels_.insert(name);
    return 0;
  }

  // Grava a publicação (Envelope) em publications.bin
  bool add_publication(const chat::Envelope& e) {
    std::string buf;
    if (!e.SerializeToString(&buf)) return false;
    uint32_t n = static_cast<uint32_t>(buf.size());
    const char hdr[4] = {static_cast<char>(n >> 24), static_cast<char>(n >> 16),
                         static_cast<char>(n >> 8), static_cast<char>(n)};
    std::ofstream out(dir_ + "/publications.bin", std::ios::app | std::ios::binary);
    out.write(hdr, 4);
    out.write(buf.data(), static_cast<std::streamsize>(n));
    out.flush();
    return static_cast<bool>(out);
  }

  const std::set<std::string>& channels() const { return channels_; }

 private:
  std::string dir_;
  std::set<std::string> channels_;
};

// Lê publications.bin e imprime o conteúdo de cada registro.
static int dump_publications(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    std::cerr << "não foi possível abrir " << path << std::endl;
    return 1;
  }
  int count = 0;
  unsigned char hdr[4];
  while (in.read(reinterpret_cast<char*>(hdr), 4)) {
    uint32_t n = (uint32_t(hdr[0]) << 24) | (uint32_t(hdr[1]) << 16) | (uint32_t(hdr[2]) << 8) | hdr[3];
    std::string buf(n, '\0');
    if (!in.read(&buf[0], n)) {
      std::cerr << "registro truncado no final do arquivo" << std::endl;
      return 1;
    }
    chat::Envelope e;
    if (!e.ParseFromString(buf)) {
      std::cerr << "registro " << count << " inválido" << std::endl;
      return 1;
    }
    log_msg("dump", "ARMAZENADO", e);
    ++count;
  }
  std::cout << count << " publicação(ões) no arquivo" << std::endl;
  return 0;
}

static std::string fmt1(double v) {
  char b[32];
  snprintf(b, sizeof b, "%.1f", v);
  return b;
}

// Algoritmo de Cristian: a hora de quem respondeu chegou depois de ~metade do tempo de ida e volta,
// então a hora correta agora é (hora remota + rtt/2). O deslocamento é calculado sobre a hora ORIGINAL
// da máquina (now_ms), nunca sobre a já corrigida, para não acumular erro.
static void apply_sync(const std::string& who, const std::string& from, int64_t remote_ms, double rtt_ms,
                       int64_t local_at_receive) {
  constexpr double kMaxSyncRttMs = 500.0;
  if (rtt_ms > kMaxSyncRttMs) {
    println_locked("[" + who + "] sincronização descartada: ida e volta de " + fmt1(rtt_ms) +
                   " ms é grande demais para uma boa estimativa");
    return;
  }
  int64_t estimated = remote_ms + static_cast<int64_t>(std::llround(rtt_ms / 2.0));
  int64_t new_offset = estimated - local_at_receive;
  int64_t old_offset = g_clock_offset_ms.exchange(new_offset);
  println_locked("[" + who + "] relógio sincronizado com " + from + ": deslocamento " + std::to_string(old_offset) +
                 " ms -> " + std::to_string(new_offset) + " ms (ida e volta: " + fmt1(rtt_ms) + " ms)");
}

// Cliente do serviço de referência (REQ/REP). Se a referência não responde, o socket REQ é
// recriado (lazy pirate) e o servidor continua atendendo os bots normalmente.
class ReferenceClient {
 public:
  ReferenceClient(std::string endpoint, std::string who) : ctx_(1), ep_(std::move(endpoint)), who_(std::move(who)) {
    reset();
  }

  // REQ: nome -> REP: rank (isso também cadastra o servidor na referência)
  bool register_server(int* rank) {
    chat::Envelope req = make_envelope(who_);
    req.mutable_rank_request()->set_name(who_);
    chat::Envelope rep;
    if (!call(req, &rep, nullptr, nullptr) || !rep.has_rank_response()) return false;
    *rank = rep.rank_response().rank();
    return true;
  }

  // REQ: heartbeat -> REP: OK. Retorna 1 = ok, 0 = sem resposta válida,
  // -1 = a referência não conhece este servidor (success=false).
  // (Parte 4: a hora não vem mais na resposta do heartbeat.)
  int heartbeat() {
    chat::Envelope req = make_envelope(who_);
    req.mutable_heartbeat_request()->set_name(who_);
    chat::Envelope rep;
    if (!call(req, &rep, nullptr, nullptr) || !rep.has_heartbeat_response()) return 0;
    return rep.heartbeat_response().success() ? 1 : -1;
  }

  // REQ: list -> REP: [nomes e ranks]
  bool list_servers(std::vector<std::pair<std::string, int>>* out = nullptr) {
    chat::Envelope req = make_envelope(who_);
    req.mutable_list_servers_request();
    chat::Envelope rep;
    if (!call(req, &rep, nullptr, nullptr) || !rep.has_list_servers_response()) return false;
    if (out) {
      out->clear();
      for (const auto& sv : rep.list_servers_response().servers()) out->emplace_back(sv.name(), sv.rank());
    }
    return true;
  }

 private:
  // Envia a requisição e espera a resposta. rtt_ms / local_at_receive (opcionais) medem o tempo
  // entre o send e o recv e a hora da máquina no instante do recv, sem incluir serialização/log.
  bool call(chat::Envelope req, chat::Envelope* rep, double* rtt_ms, int64_t* local_at_receive) {
    stamp(req);
    log_msg(who_, "ENVIADO", req);
    std::string buf;
    if (!req.SerializeToString(&buf)) return false;

    auto t0 = std::chrono::steady_clock::now();
    if (!sock_->send(zmq::buffer(buf), zmq::send_flags::none)) {
      reset();
      return false;
    }
    zmq::message_t m;
    auto r = sock_->recv(m, zmq::recv_flags::none);
    auto t1 = std::chrono::steady_clock::now();
    int64_t local_now = now_ms();

    if (!r) {
      println_locked("[" + who_ + "] referência sem resposta em " + ep_ + ", recriando socket");
      reset();
      return false;
    }
    if (!rep->ParseFromArray(m.data(), static_cast<int>(m.size()))) {
      println_locked("[" + who_ + "] resposta inválida da referência");
      return false;
    }
    g_clock.observe(rep->logical_clock());  // relógio lógico: max(local, recebido)
    if (rtt_ms) *rtt_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    if (local_at_receive) *local_at_receive = local_now;
    log_msg(who_, "RECEBIDO", *rep);
    return true;
  }

  void reset() {
    sock_.reset();
    sock_ = std::make_unique<zmq::socket_t>(ctx_, zmq::socket_type::req);
    sock_->set(zmq::sockopt::linger, 0);
    sock_->set(zmq::sockopt::rcvtimeo, 1500);
    sock_->connect(ep_);
  }

  zmq::context_t ctx_;
  std::string ep_;
  std::string who_;
  std::unique_ptr<zmq::socket_t> sock_;
};

// ---------------------------------------------------------------------------
// Parte 4: comunicação entre servidores (coordenador, eleição, sincronização)
// ---------------------------------------------------------------------------
static int64_t steady_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// Estado compartilhado entre a thread principal, a que atende os outros servidores (REP)
// e a que escuta o tópico "servers" (SUB).
struct Cluster {
  std::mutex m;
  std::string coordinator;                            // vazio = ainda não há coordenador conhecido
  std::vector<std::pair<std::string, int>> peers;     // (nome, rank) de todos os servidores
  int rank = 0;
  std::atomic<bool> election_wanted{false};           // um servidor de rank menor pediu eleição
  std::atomic<int64_t> wait_until_ms{0};              // após perder uma eleição, espera o anúncio até aqui

  std::string get_coordinator() {
    std::lock_guard<std::mutex> l(m);
    return coordinator;
  }
  void set_coordinator(const std::string& c) {
    std::lock_guard<std::mutex> l(m);
    coordinator = c;
  }
  int rank_of(const std::string& n) {
    std::lock_guard<std::mutex> l(m);
    for (auto& p : peers)
      if (p.first == n) return p.second;
    return -1;
  }
};

static int peer_port_base() {
  try {
    return std::stoi(env_or("PEER_PORT_BASE", "5560"));
  } catch (...) {
    return 5560;
  }
}

static std::string peer_endpoint(const std::string& server_name, int rank) {
  return "tcp://" + env_or("PEER_HOST", server_name) + ":" + std::to_string(peer_port_base() + rank);
}

// REQ avulso a outro servidor (um socket novo por chamada: se o par caiu, nada fica travado).
// Mede o RTT só entre o send e o recv.
static bool peer_request(zmq::context_t& ctx, const std::string& who, const std::string& ep, chat::Envelope req,
                         chat::Envelope* rep, double* rtt_ms, int64_t* local_at_receive, int timeout_ms) {
  stamp(req);
  log_msg(who, "ENVIADO", req);
  std::string buf;
  if (!req.SerializeToString(&buf)) return false;
  try {
    zmq::socket_t s(ctx, zmq::socket_type::req);
    s.set(zmq::sockopt::linger, 0);
    s.set(zmq::sockopt::rcvtimeo, timeout_ms);
    s.set(zmq::sockopt::sndtimeo, timeout_ms);
    s.connect(ep);
    auto t0 = std::chrono::steady_clock::now();
    if (!s.send(zmq::buffer(buf), zmq::send_flags::none)) return false;
    zmq::message_t m;
    auto r = s.recv(m, zmq::recv_flags::none);
    auto t1 = std::chrono::steady_clock::now();
    int64_t local_now = now_ms();
    if (!r || !rep->ParseFromArray(m.data(), static_cast<int>(m.size()))) return false;
    g_clock.observe(rep->logical_clock());
    if (rtt_ms) *rtt_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    if (local_at_receive) *local_at_receive = local_now;
    log_msg(who, "RECEBIDO", *rep);
    return true;
  } catch (const zmq::error_t& e) {
    println_locked("[" + who + "] erro ao falar com " + ep + ": " + e.what());
    return false;
  }
}

// Pede eleição a todos os servidores de rank maior ao mesmo tempo. Retorna true se algum respondeu OK.
static bool ask_higher_servers(zmq::context_t& ctx, const std::string& who, int my_rank,
                               const std::vector<std::pair<std::string, int>>& higher, int timeout_ms) {
  std::vector<std::unique_ptr<zmq::socket_t>> socks;
  for (const auto& h : higher) {
    try {
      auto s = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::req);
      s->set(zmq::sockopt::linger, 0);
      s->connect(peer_endpoint(h.first, h.second));
      chat::Envelope req = make_envelope(who);
      auto* er = req.mutable_election_request();
      er->set_name(who);
      er->set_rank(my_rank);
      stamp(req);
      log_msg(who, "ENVIADO", req);
      std::string buf;
      req.SerializeToString(&buf);
      s->send(zmq::buffer(buf), zmq::send_flags::dontwait);
      socks.push_back(std::move(s));
    } catch (const zmq::error_t&) {
    }
  }
  const int64_t deadline = steady_ms() + timeout_ms;
  std::vector<bool> done(socks.size(), false);
  while (true) {
    int64_t left = deadline - steady_ms();
    if (left <= 0) return false;
    std::vector<zmq_pollitem_t> items;
    std::vector<size_t> idx;
    for (size_t i = 0; i < socks.size(); ++i)
      if (!done[i]) {
        items.push_back({socks[i]->handle(), 0, ZMQ_POLLIN, 0});
        idx.push_back(i);
      }
    if (items.empty()) return false;
    int n = zmq_poll(items.data(), static_cast<int>(items.size()), static_cast<long>(left));
    if (n <= 0) return false;
    for (size_t k = 0; k < items.size(); ++k) {
      if (!(items[k].revents & ZMQ_POLLIN)) continue;
      done[idx[k]] = true;
      zmq::message_t m;
      chat::Envelope rep;
      if (!socks[idx[k]]->recv(m, zmq::recv_flags::dontwait) ||
          !rep.ParseFromArray(m.data(), static_cast<int>(m.size())))
        continue;
      g_clock.observe(rep.logical_clock());
      log_msg(who, "RECEBIDO", rep);
      if (rep.has_election_response() && rep.election_response().ok()) return true;
    }
  }
}

// Thread: atende os pedidos dos outros servidores (clock_request e election_request).
static void peer_server_loop(zmq::context_t& ctx, std::string who, int rank, Cluster& cl) {
  const std::string bind = "tcp://*:" + std::to_string(peer_port_base() + rank);
  zmq::socket_t rep(ctx, zmq::socket_type::rep);
  rep.set(zmq::sockopt::linger, 0);
  rep.set(zmq::sockopt::rcvtimeo, 200);
  try {
    rep.bind(bind);
  } catch (const zmq::error_t& e) {
    println_locked("[" + who + "] não foi possível escutar em " + bind + ": " + e.what());
    return;
  }
  println_locked("[" + who + "] atendendo outros servidores em " + bind);
  while (true) {
    chat::Envelope req;
    int r = recv_envelope(rep, req);
    if (r == 0) continue;
    chat::Envelope resp = make_envelope(who);
    if (r == 1) {
      log_msg(who, "RECEBIDO", req);
      if (req.has_clock_request()) {
        resp.mutable_clock_response();
      } else if (req.has_election_request()) {
        // "OK" só se quem pede tem rank menor; neste caso eu assumo a eleição (próximo ciclo da thread principal)
        bool ok = req.election_request().rank() < rank;
        resp.mutable_election_response()->set_ok(ok);
        if (ok) cl.election_wanted = true;
      } else {
        resp.mutable_election_response()->set_ok(false);
      }
    } else {
      resp.mutable_election_response()->set_ok(false);
    }
    stamp(resp);
    if (resp.has_clock_response()) resp.mutable_clock_response()->set_time_ms(resp.timestamp_ms());
    send_envelope(rep, resp);
    log_msg(who, "ENVIADO", resp);
  }
}

// Thread: assina o tópico "servers" (proxy XPUB) e guarda o coordenador anunciado.
static void coordinator_listener(std::string who, std::string xpub, Cluster& cl) {
  zmq::context_t ctx(1);
  zmq::socket_t sub(ctx, zmq::socket_type::sub);
  sub.set(zmq::sockopt::linger, 0);
  sub.set(zmq::sockopt::rcvtimeo, 500);
  sub.set(zmq::sockopt::subscribe, "servers");
  sub.connect(xpub);
  while (true) {
    zmq::message_t topic, body;
    if (!sub.recv(topic, zmq::recv_flags::none)) continue;
    if (!topic.more() || !sub.recv(body, zmq::recv_flags::none)) continue;
    // A inscrição casa por prefixo: só aceitamos o tópico exato.
    if (std::string(static_cast<const char*>(topic.data()), topic.size()) != "servers") continue;
    chat::Envelope e;
    if (!e.ParseFromArray(body.data(), static_cast<int>(body.size())) || !e.has_coordinator_announcement()) continue;
    g_clock.observe(e.logical_clock());
    log_msg(who, "RECEBIDO", e);
    const std::string& coord = e.coordinator_announcement().name();
    if (coord == who) continue;
    std::string current = cl.get_coordinator();
    // Alguém de rank menor se anunciou enquanto eu sou o coordenador: contesto (volta a haver eleição).
    if (current == who && cl.rank_of(coord) >= 0 && cl.rank_of(coord) < cl.rank) {
      cl.election_wanted = true;
      continue;
    }
    cl.set_coordinator(coord);
    println_locked("[" + who + "] coordenador atual: " + coord);
  }
}

template <typename Resp>
static void fill(Resp* r, bool ok, chat::ErrorCode code, const std::string& msg) {
  r->set_success(ok);
  r->set_error(code);
  r->set_error_message(msg);
}

int main(int argc, char** argv) {
  if (argc == 3 && std::string(argv[1]) == "--dump") return dump_publications(argv[2]);

  const std::string name = env_or("SERVER_NAME", "server_cpp");
  const std::string backend = env_or("BROKER_BACKEND", "tcp://broker:5556");
  const std::string pubsub = env_or("PROXY_XSUB", "tcp://proxy:5557");
  // Mesmo nome de variável usado pelos servidores Java (REFERENCE_ADDR); REFERENCE_ENDPOINT também vale.
  const std::string reference_ep =
      env_or("REFERENCE_ADDR", env_or("REFERENCE_ENDPOINT", "tcp://reference:5559"));
  const std::string data_dir = env_or("DATA_DIR", "/data");
  int heartbeat_every = 10;  // enunciado: a cada 10 mensagens de clientes (ajustável só para testes)
  try {
    heartbeat_every = std::max(1, std::stoi(env_or("HEARTBEAT_EVERY", "10")));
  } catch (...) {
  }

  int clock_sync_every = 15;  // enunciado (Parte 4): atualiza o relógio a cada 15 mensagens
  try {
    clock_sync_every = std::max(1, std::stoi(env_or("CLOCK_SYNC_EVERY", "15")));
  } catch (...) {
  }
  const std::string xpub = env_or("PROXY_XPUB", "tcp://proxy:5558");

  Storage st(data_dir);

  zmq::context_t ctx(1);
  zmq::socket_t sock(ctx, zmq::socket_type::rep);  // requisições dos bots (via broker)
  sock.set(zmq::sockopt::rcvtimeo, 200);  // acorda a cada 200 ms para cuidar de eleições mesmo sem clientes
  sock.connect(backend);
  zmq::socket_t pub(ctx, zmq::socket_type::pub);   // publicações nos canais (via proxy)
  pub.connect(pubsub);
  std::cout << "[" << name << "] conectado ao broker em " << backend << " e ao proxy Pub/Sub em "
            << pubsub << " | referência em " << reference_ep << " | dados em " << data_dir
            << " | canais carregados: " << st.channels().size() << std::endl;

  // --- Parte 3: cadastro na referência (rank) ---
  Cluster cl;
  std::thread(coordinator_listener, name, xpub, std::ref(cl)).detach();  // escuta o tópico "servers"

  ReferenceClient ref(reference_ep, name);
  bool registered = false;
  bool peer_thread_started = false;
  int rank = 0;
  auto refresh_peers = [&]() {
    std::vector<std::pair<std::string, int>> v;
    if (ref.list_servers(&v)) {
      std::lock_guard<std::mutex> l(cl.m);
      cl.peers = v;
      return true;
    }
    return false;
  };
  auto try_register = [&]() {
    if (ref.register_server(&rank)) {
      registered = true;
      cl.rank = rank;
      println_locked("[" + name + "] rank recebido da referência: " + std::to_string(rank));
      if (!peer_thread_started) {  // passa a atender os outros servidores em PEER_PORT_BASE + rank
        peer_thread_started = true;
        std::thread(peer_server_loop, std::ref(ctx), name, rank, std::ref(cl)).detach();
      }
    }
  };
  // Poucas tentativas na partida (o servidor não deve ficar parado esperando a referência);
  // se não der, tenta de novo nos próximos heartbeats.
  for (int attempt = 1; attempt <= 5 && !registered; ++attempt) {
    try_register();
    if (!registered) std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  if (registered) {
    refresh_peers();
  } else {
    println_locked("[" + name + "] referência indisponível; atendendo sem rank (nova tentativa nos próximos heartbeats)");
  }

  // --- Parte 4: coordenador, eleição e sincronização ---
  // Publica no tópico "servers" que este servidor é o coordenador.
  auto announce = [&]() {
    chat::Envelope e = make_envelope(name);
    e.mutable_coordinator_announcement()->set_name(name);
    stamp(e);
    std::string payload;
    e.SerializeToString(&payload);
    pub.send(zmq::buffer(std::string("servers")), zmq::send_flags::sndmore);
    pub.send(zmq::buffer(payload), zmq::send_flags::none);
    log_msg(name, "PUBLICADO", e);
  };

  // Bully: pede eleição aos servidores de rank maior; se nenhum responde OK, este servidor assume.
  auto run_election = [&]() {
    refresh_peers();
    std::vector<std::pair<std::string, int>> higher;
    {
      std::lock_guard<std::mutex> l(cl.m);
      for (auto& p : cl.peers)
        if (p.second > rank) higher.push_back(p);
    }
    println_locked("[" + name + "] eleição iniciada (rank " + std::to_string(rank) + "): consultando " +
                   std::to_string(higher.size()) + " servidor(es) de rank maior");
    if (!higher.empty() && ask_higher_servers(ctx, name, rank, higher, 1000)) {
      cl.set_coordinator("");
      cl.wait_until_ms = steady_ms() + 4000;  // alguém de rank maior assume e vai anunciar
      println_locked("[" + name + "] um servidor de rank maior respondeu OK; aguardando o anúncio do coordenador");
    } else {
      cl.set_coordinator(name);
      println_locked("[" + name + "] eleito coordenador");
      announce();
    }
  };

  // Chamada a cada volta do laço: eleição pedida por outro servidor, ou ainda sem coordenador.
  auto maintain = [&]() {
    if (!registered) return;
    bool wanted = cl.election_wanted.exchange(false);
    bool none = cl.get_coordinator().empty() && steady_ms() >= cl.wait_until_ms.load();
    if (wanted || none) run_election();
  };

  // A cada clock_sync_every mensagens: pede a hora ao coordenador (Cristian com o RTT medido).
  auto sync_with_coordinator = [&]() {
    if (!registered) return;
    std::string coord = cl.get_coordinator();
    if (coord.empty()) return;  // maintain() cuida da eleição
    if (coord == name) {
      announce();  // reforça o anúncio (quem entrou depois pode não ter recebido o primeiro)
      return;
    }
    int crank = cl.rank_of(coord);
    if (crank < 0) {
      refresh_peers();
      crank = cl.rank_of(coord);
    }
    chat::Envelope req = make_envelope(name);
    req.mutable_clock_request();
    chat::Envelope rep;
    double rtt = 0;
    int64_t local_rcv = 0;
    if (crank >= 0 && peer_request(ctx, name, peer_endpoint(coord, crank), req, &rep, &rtt, &local_rcv, 1000) &&
        rep.has_clock_response()) {
      apply_sync(name, coord, rep.clock_response().time_ms(), rtt, local_rcv);
    } else {
      println_locked("[" + name + "] coordenador " + coord + " não respondeu; iniciando eleição");
      cl.set_coordinator("");
      run_election();
    }
  };

  int since_heartbeat = 0;
  int since_sync = 0;
  // A cada heartbeat_every mensagens de clientes: heartbeat e lista de servidores.
  // A cada clock_sync_every: sincronização com o coordenador.
  auto after_client_message = [&]() {
    if (++since_sync >= clock_sync_every) {
      since_sync = 0;
      sync_with_coordinator();
    }
    if (++since_heartbeat < heartbeat_every) return;
    since_heartbeat = 0;
    bool reference_ok = false;  // só pede a lista se a referência respondeu agora
    if (!registered) {
      try_register();
      reference_ok = registered;
    } else {
      int hb = ref.heartbeat();
      if (hb == 1) {
        reference_ok = true;
      } else if (hb == -1) {
        println_locked("[" + name + "] a referência não conhece este servidor; cadastrando de novo");
        registered = false;
        try_register();
        reference_ok = registered;
      }
    }
    if (reference_ok) refresh_peers();
  };

  while (true) {
    chat::Envelope req;
    int r = recv_envelope(sock, req);
    chat::Envelope rep = make_envelope(name);

    if (r == -1) {
      std::cerr << "[" << name << "] mensagem inválida recebida" << std::endl;
      fill(rep.mutable_login_response(), false, chat::ERROR_UNKNOWN_REQUEST,
           "mensagem não é um Envelope válido");
      stamp(rep);
      send_envelope(sock, rep);
      after_client_message();
      continue;
    }
    if (r == 0) {
      maintain();
      continue;
    }

    log_msg(name, "RECEBIDO", req);

    switch (req.body_case()) {
      case chat::Envelope::kLoginRequest: {
        const std::string& user = req.login_request().username();
        auto* resp = rep.mutable_login_response();
        if (!valid_name(user)) {
          fill(resp, false, chat::ERROR_INVALID_USERNAME,
               "usuário deve ter 1-32 caracteres [A-Za-z0-9_-]");
        } else if (!st.add_login(user, clock_ms())) {
          fill(resp, false, chat::ERROR_INTERNAL, "falha ao gravar login em disco");
        } else {
          fill(resp, true, chat::ERROR_NONE, "");
        }
        break;
      }
      case chat::Envelope::kCreateChannelRequest: {
        const std::string& ch = req.create_channel_request().channel_name();
        auto* resp = rep.mutable_create_channel_response();
        if (!valid_name(ch)) {
          fill(resp, false, chat::ERROR_INVALID_CHANNEL_NAME,
               "canal deve ter 1-32 caracteres [A-Za-z0-9_-]");
        } else {
          int res = st.add_channel(ch);
          if (res == 0) fill(resp, true, chat::ERROR_NONE, "");
          else if (res == 1) fill(resp, false, chat::ERROR_CHANNEL_EXISTS, "canal já existe");
          else fill(resp, false, chat::ERROR_INTERNAL, "falha ao gravar canal em disco");
        }
        break;
      }
      case chat::Envelope::kListChannelsRequest: {
        auto* resp = rep.mutable_list_channels_response();
        for (const auto& c : st.channels()) resp->add_channels(c);
        break;
      }
      case chat::Envelope::kPublishRequest: {
        const std::string& ch = req.publish_request().channel();
        const std::string& text = req.publish_request().message();
        auto* resp = rep.mutable_publish_response();
        // Obs.: cada servidor conhece só os canais criados nele (dados separados), então não
        // exigimos que o canal exista aqui: basta o nome ser válido (o tópico é só o nome).
        if (!valid_name(ch)) {
          fill(resp, false, chat::ERROR_INVALID_CHANNEL_NAME,
               "canal deve ter 1-32 caracteres [A-Za-z0-9_-]");
        } else if (!valid_message(text)) {
          fill(resp, false, chat::ERROR_INVALID_MESSAGE,
               "mensagem deve ter 1-" + std::to_string(kMaxMessageLen) + " bytes");
        } else {
          // Mensagem que vai para o tópico: carimbada (hora + relógio lógico) logo antes de
          // gravar e publicar; autor = bot que pediu.
          chat::Envelope pubmsg = make_envelope(name);
          auto* p = pubmsg.mutable_publication();
          p->set_channel(ch);
          p->set_message(text);
          p->set_author(req.sender());
          stamp(pubmsg);

          std::string payload;
          if (!st.add_publication(pubmsg)) {
            fill(resp, false, chat::ERROR_INTERNAL, "falha ao gravar publicação em disco");
          } else if (!pubmsg.SerializeToString(&payload)) {
            fill(resp, false, chat::ERROR_INTERNAL, "falha ao serializar publicação");
          } else {
            // PUB: frame 1 = tópico (nome do canal), frame 2 = Envelope
            pub.send(zmq::buffer(ch), zmq::send_flags::sndmore);
            pub.send(zmq::buffer(payload), zmq::send_flags::none);
            log_msg(name, "PUBLICADO", pubmsg);
            fill(resp, true, chat::ERROR_NONE, "");
          }
        }
        break;
      }
      default:
        fill(rep.mutable_login_response(), false, chat::ERROR_UNKNOWN_REQUEST,
             "tipo de requisição não suportado");
    }

    stamp(rep);  // hora + relógio lógico incrementado, imediatamente antes de enviar
    send_envelope(sock, rep);
    log_msg(name, "ENVIADO", rep);

    after_client_message();  // sincronização/heartbeat depois de responder ao bot
    maintain();
  }
}