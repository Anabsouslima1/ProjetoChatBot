// Servidor (REP + PUB): trata login, canais e publicações.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv        -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt      -> um canal por linha
//   publications.bin  -> cada publicação: 4 bytes (big-endian) com o tamanho + Envelope serializado
//                        (protobuf). Contém canal, autor, texto e timestamp: dá para recuperar tudo.
// Para listar o conteúdo de publications.bin:  server --dump /data/publications.bin
#include <zmq.hpp>

#include <algorithm>
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

  // REQ: heartbeat -> REP: OK + hora correta. Retorna 1 = ok (relógio ajustado),
  // 0 = sem resposta válida, -1 = a referência não conhece este servidor (success=false).
  int heartbeat() {
    chat::Envelope req = make_envelope(who_);
    req.mutable_heartbeat_request()->set_name(who_);
    chat::Envelope rep;
    double rtt_ms = 0;
    int64_t local_at_receive = 0;
    if (!call(req, &rep, &rtt_ms, &local_at_receive) || !rep.has_heartbeat_response()) return 0;
    const auto& h = rep.heartbeat_response();
    if (!h.success()) return -1;
    sync_clock(h.time_ms(), rtt_ms, local_at_receive);
    return 1;
  }

  // REQ: list -> REP: [nomes e ranks] (o conteúdo aparece no log da resposta)
  bool list_servers() {
    chat::Envelope req = make_envelope(who_);
    req.mutable_list_servers_request();
    chat::Envelope rep;
    return call(req, &rep, nullptr, nullptr) && rep.has_list_servers_response();
  }

 private:
  // Algoritmo de Cristian: a hora da referência chegou depois de ~metade do tempo de ida e volta,
  // então a hora correta agora é (hora da referência + rtt/2). O deslocamento é calculado sobre a
  // hora ORIGINAL da máquina (now_ms), nunca sobre a já corrigida, para não acumular erro.
  void sync_clock(int64_t reference_ms, double rtt_ms, int64_t local_at_receive) {
    if (rtt_ms > kMaxSyncRttMs) {
      println_locked("[" + who_ + "] sincronização descartada: ida e volta de " + fmt1(rtt_ms) +
                     " ms é grande demais para uma boa estimativa");
      return;
    }
    int64_t estimated = reference_ms + static_cast<int64_t>(std::llround(rtt_ms / 2.0));
    int64_t new_offset = estimated - local_at_receive;
    int64_t old_offset = g_clock_offset_ms.exchange(new_offset);
    println_locked("[" + who_ + "] relógio sincronizado: deslocamento " + std::to_string(old_offset) +
                   " ms -> " + std::to_string(new_offset) + " ms (ida e volta: " + fmt1(rtt_ms) + " ms)");
  }

  static std::string fmt1(double v) {
    char b[32];
    snprintf(b, sizeof b, "%.1f", v);
    return b;
  }

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

  static constexpr double kMaxSyncRttMs = 500.0;

  zmq::context_t ctx_;
  std::string ep_;
  std::string who_;
  std::unique_ptr<zmq::socket_t> sock_;
};

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

  Storage st(data_dir);

  zmq::context_t ctx(1);
  zmq::socket_t sock(ctx, zmq::socket_type::rep);  // requisições dos bots (via broker)
  sock.connect(backend);
  zmq::socket_t pub(ctx, zmq::socket_type::pub);   // publicações nos canais (via proxy)
  pub.connect(pubsub);
  std::cout << "[" << name << "] conectado ao broker em " << backend << " e ao proxy Pub/Sub em "
            << pubsub << " | referência em " << reference_ep << " | dados em " << data_dir
            << " | canais carregados: " << st.channels().size() << std::endl;

  // --- Parte 3: cadastro na referência (rank) ---
  ReferenceClient ref(reference_ep, name);
  bool registered = false;
  int rank = 0;
  auto try_register = [&]() {
    if (ref.register_server(&rank)) {
      registered = true;
      println_locked("[" + name + "] rank recebido da referência: " + std::to_string(rank));
    }
  };
  // Poucas tentativas na partida (o servidor não deve ficar parado esperando a referência);
  // se não der, tenta de novo nos próximos heartbeats.
  for (int attempt = 1; attempt <= 5 && !registered; ++attempt) {
    try_register();
    if (!registered) std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  if (registered) {
    ref.list_servers();
  } else {
    println_locked("[" + name + "] referência indisponível; atendendo sem rank (nova tentativa nos próximos heartbeats)");
  }

  int since_heartbeat = 0;
  // A cada heartbeat_every mensagens de clientes: heartbeat (+ hora correta) e lista de servidores.
  auto after_client_message = [&]() {
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
    if (reference_ok) ref.list_servers();
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
    if (r == 0) continue;

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

    after_client_message();  // heartbeat depois de responder ao bot
  }
}