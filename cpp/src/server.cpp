// Servidor (REP + PUB): trata login, canais e publicações.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv        -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt      -> um canal por linha
//   publications.bin  -> cada publicação: 4 bytes (big-endian) com o tamanho + Envelope serializado
//                        (protobuf). Contém canal, autor, texto e timestamp: dá para recuperar tudo.
// Para listar o conteúdo de publications.bin:  server --dump /data/publications.bin
#include <zmq.hpp>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <set>
#include <sys/stat.h>
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
  const std::string data_dir = env_or("DATA_DIR", "/data");

  Storage st(data_dir);

  zmq::context_t ctx(1);
  zmq::socket_t sock(ctx, zmq::socket_type::rep);  // requisições dos bots (via broker)
  sock.connect(backend);
  zmq::socket_t pub(ctx, zmq::socket_type::pub);   // publicações nos canais (via proxy)
  pub.connect(pubsub);
  std::cout << "[" << name << "] conectado ao broker em " << backend << " e ao proxy Pub/Sub em "
            << pubsub << " | dados em " << data_dir << " | canais carregados: " << st.channels().size()
            << std::endl;

  while (true) {
    chat::Envelope req;
    int r = recv_envelope(sock, req);
    chat::Envelope rep = make_envelope(name);

    if (r == -1) {
      std::cerr << "[" << name << "] mensagem inválida recebida" << std::endl;
      fill(rep.mutable_login_response(), false, chat::ERROR_UNKNOWN_REQUEST,
           "mensagem não é um Envelope válido");
      send_envelope(sock, rep);
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
        } else if (!st.add_login(user, rep.timestamp_ms())) {
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
          // Mensagem que vai para o tópico: timestamp do envio da publicação, autor = bot.
          chat::Envelope pubmsg = make_envelope(name);
          auto* p = pubmsg.mutable_publication();
          p->set_channel(ch);
          p->set_message(text);
          p->set_author(req.sender());

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

    rep.set_timestamp_ms(now_ms());
    send_envelope(sock, rep);
    log_msg(name, "ENVIADO", rep);
  }
}