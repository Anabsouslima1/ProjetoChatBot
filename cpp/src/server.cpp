// Servidor (REP): trata login, criação e listagem de canais.
// Persistência (por servidor, em DATA_DIR):
//   logins.tsv   -> "<timestamp_ms>\t<usuario>" por login
//   channels.txt -> um canal por linha
#include <zmq.hpp>

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

  const std::set<std::string>& channels() const { return channels_; }

 private:
  std::string dir_;
  std::set<std::string> channels_;
};

template <typename Resp>
static void fill(Resp* r, bool ok, chat::ErrorCode code, const std::string& msg) {
  r->set_success(ok);
  r->set_error(code);
  r->set_error_message(msg);
}

int main() {
  const std::string name = env_or("SERVER_NAME", "server_cpp");
  const std::string backend = env_or("BROKER_BACKEND", "tcp://broker:5556");
  const std::string data_dir = env_or("DATA_DIR", "/data");

  Storage st(data_dir);

  zmq::context_t ctx(1);
  zmq::socket_t sock(ctx, zmq::socket_type::rep);
  sock.connect(backend);
  std::cout << "[" << name << "] conectado ao broker em " << backend << " | dados em " << data_dir
            << " | canais carregados: " << st.channels().size() << std::endl;

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
      default:
        fill(rep.mutable_login_response(), false, chat::ERROR_UNKNOWN_REQUEST,
             "tipo de requisição não suportado");
    }

    rep.set_timestamp_ms(now_ms());
    send_envelope(sock, rep);
    log_msg(name, "ENVIADO", rep);
  }
}
