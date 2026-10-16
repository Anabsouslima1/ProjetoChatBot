// Cliente (bot): faz login, lista canais e cria canais que ainda não existem.
#include <zmq.hpp>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "common.hpp"

// REQ com timeout: se o servidor não responder, o socket REQ fica travado, então
// ele é recriado (padrão "lazy pirate").
class Rpc {
 public:
  Rpc(std::string endpoint, std::string who) : ctx_(1), ep_(std::move(endpoint)), who_(std::move(who)) {
    reset();
  }

  bool call(const chat::Envelope& req, chat::Envelope& rep) {
    log_msg(who_, "ENVIADO", req);
    if (!send_envelope(*sock_, req)) {
      reset();
      return false;
    }
    int r = recv_envelope(*sock_, rep);
    if (r != 1) {
      std::cerr << who_ << " | sem resposta válida (r=" << r << "), recriando socket" << std::endl;
      reset();
      return false;
    }
    log_msg(who_, "RECEBIDO", rep);
    return true;
  }

 private:
  void reset() {
    sock_.reset();
    sock_ = std::make_unique<zmq::socket_t>(ctx_, zmq::socket_type::req);
    sock_->set(zmq::sockopt::linger, 0);
    sock_->set(zmq::sockopt::rcvtimeo, 5000);
    sock_->connect(ep_);
  }

  zmq::context_t ctx_;
  std::string ep_;
  std::string who_;
  std::unique_ptr<zmq::socket_t> sock_;
};

static void sleep_s(int s) { std::this_thread::sleep_for(std::chrono::seconds(s)); }

int main() {
  const std::string name = env_or("BOT_NAME", "bot_cpp");
  const std::string endpoint = env_or("BROKER_FRONTEND", "tcp://broker:5555");
  const std::string own_channel = env_or("BOT_CHANNEL", "canal-" + name);

  Rpc rpc(endpoint, name);

  // 1) Login (repetido até dar certo)
  while (true) {
    chat::Envelope req = make_envelope(name);
    req.mutable_login_request()->set_username(name);
    chat::Envelope rep;
    if (rpc.call(req, rep) && rep.has_login_response() && rep.login_response().success()) {
      std::cout << name << " | login realizado com sucesso" << std::endl;
      break;
    }
    std::cout << name << " | login falhou, tentando novamente em 2s" << std::endl;
    sleep_s(2);
  }

  // 2) Lista canais e cria os que faltam
  const std::vector<std::string> wanted = {"geral", own_channel};
  while (true) {
    chat::Envelope req = make_envelope(name);
    req.mutable_list_channels_request();
    chat::Envelope rep;
    if (rpc.call(req, rep) && rep.has_list_channels_response()) {
      std::vector<std::string> existing(rep.list_channels_response().channels().begin(),
                                        rep.list_channels_response().channels().end());
      for (const auto& ch : wanted) {
        if (std::find(existing.begin(), existing.end(), ch) != existing.end()) continue;
        chat::Envelope creq = make_envelope(name);
        creq.mutable_create_channel_request()->set_channel_name(ch);
        chat::Envelope crep;
        rpc.call(creq, crep);
      }
    }
    sleep_s(5);
  }
}