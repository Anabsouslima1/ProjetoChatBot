// Cliente (bot).
// Ao conectar:  login -> loop infinito de rodadas. Em cada rodada o bot:
//   1. lista os canais; se houver menos de 5, cria um novo;
//   2. se estiver inscrito em menos de 3 canais, se inscreve em mais um (escolhido ao acaso);
//   3. escolhe um canal da lista e publica 10 mensagens aleatórias com 1 s de intervalo.
// Uma thread própria recebe (SUB) e exibe tudo que chega nos canais em que o bot está inscrito.
#include <zmq.hpp>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <set>
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
      println_locked(who_ + " | sem resposta válida (r=" + std::to_string(r) + "), recriando socket");
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

// Assinante (SUB) em thread própria. Um socket ZeroMQ não pode ser usado por duas threads,
// então a thread principal só registra o canal desejado; a inscrição de fato é feita aqui.
class Subscriber {
 public:
  Subscriber(std::string endpoint, std::string who) : ep_(std::move(endpoint)), who_(std::move(who)) {}

  void start() {
    std::thread(&Subscriber::run, this).detach();
  }

  void subscribe(const std::string& channel) {
    std::lock_guard<std::mutex> lock(m_);
    if (subscribed_.insert(channel).second) pending_.push_back(channel);
  }

  size_t count() {
    std::lock_guard<std::mutex> lock(m_);
    return subscribed_.size();
  }

  bool is_subscribed(const std::string& channel) {
    std::lock_guard<std::mutex> lock(m_);
    return subscribed_.count(channel) > 0;
  }

 private:
  void run() {
    zmq::context_t ctx(1);
    zmq::socket_t sub(ctx, zmq::socket_type::sub);
    sub.set(zmq::sockopt::rcvtimeo, 200);
    sub.set(zmq::sockopt::linger, 0);
    sub.connect(ep_);

    while (true) {
      std::vector<std::string> todo;
      {
        std::lock_guard<std::mutex> lock(m_);
        todo.swap(pending_);
      }
      for (const auto& c : todo) zmq_setsockopt(sub.handle(), ZMQ_SUBSCRIBE, c.data(), c.size());

      // Mensagem do Pub/Sub: frame 1 = tópico (canal), frame 2 = Envelope
      zmq::message_t topic, body;
      if (!sub.recv(topic, zmq::recv_flags::none)) continue;
      if (!topic.more() || !sub.recv(body, zmq::recv_flags::none)) continue;

      const int64_t received_at = now_ms();
      std::string channel(static_cast<const char*>(topic.data()), topic.size());
      // A inscrição do ZeroMQ casa por prefixo ("geral" também recebe "geral2"):
      // só exibimos canais em que o bot realmente se inscreveu.
      if (!is_subscribed(channel)) continue;

      chat::Envelope e;
      if (!e.ParseFromArray(body.data(), static_cast<int>(body.size())) || !e.has_publication()) continue;

      println_locked(fmt_ts(received_at) + " | " + who_ + " | MENSAGEM | canal=" + channel +
                     " | de=" + e.publication().author() + " | enviada_em=" + fmt_ts(e.timestamp_ms()) +
                     " | recebida_em=" + fmt_ts(received_at) + " | \"" + e.publication().message() + "\"");
    }
  }

  std::string ep_;
  std::string who_;
  std::mutex m_;
  std::set<std::string> subscribed_;
  std::vector<std::string> pending_;
};

static void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

static int env_int(const char* key, int fallback) {
  try {
    return std::stoi(env_or(key, std::to_string(fallback)));
  } catch (...) {
    return fallback;
  }
}

int main() {
  const std::string name = env_or("BOT_NAME", "bot_cpp");
  const std::string endpoint = env_or("BROKER_FRONTEND", "tcp://broker:5555");
  const std::string pubsub = env_or("PROXY_XPUB", "tcp://proxy:5558");
  // Valores padrão do enunciado: 10 mensagens, 1 s de intervalo (ajustáveis só para testes rápidos)
  const int msgs_per_round = env_int("MSGS_PER_ROUND", 10);
  const int msg_interval_ms = env_int("MSG_INTERVAL_MS", 1000);

  std::mt19937 rng{std::random_device{}()};
  auto pick = [&rng](size_t n) { return std::uniform_int_distribution<size_t>(0, n - 1)(rng); };

  Rpc rpc(endpoint, name);
  Subscriber sub(pubsub, name);
  sub.start();

  // Login (repetido até dar certo)
  while (true) {
    chat::Envelope req = make_envelope(name);
    req.mutable_login_request()->set_username(name);
    chat::Envelope rep;
    if (rpc.call(req, rep) && rep.has_login_response() && rep.login_response().success()) {
      println_locked(name + " | login realizado com sucesso");
      break;
    }
    println_locked(name + " | login falhou, tentando novamente em 2s");
    sleep_ms(2000);
  }

  const std::vector<std::string> phrases = {
      "ola pessoal!",        "alguem por ai?",        "testando o canal",     "mensagem aleatoria",
      "sistemas distribuidos", "zeromq e protobuf",    "bom dia, canal!",      "publicando de novo",
      "ping",                "tudo certo por aqui"};

  while (true) {
    // Lista os canais disponíveis
    chat::Envelope lreq = make_envelope(name);
    lreq.mutable_list_channels_request();
    chat::Envelope lrep;
    if (!rpc.call(lreq, lrep) || !lrep.has_list_channels_response()) {
      sleep_ms(2000);
      continue;
    }
    std::vector<std::string> channels(lrep.list_channels_response().channels().begin(),
                                      lrep.list_channels_response().channels().end());

    // 1) menos de 5 canais -> cria um novo
    if (channels.size() < 5) {
      std::string base = "canal-" + name;
      if (base.size() > 27) base.resize(27);
      std::string novo = base + "-" + std::to_string(1000 + pick(9000));
      chat::Envelope creq = make_envelope(name);
      creq.mutable_create_channel_request()->set_channel_name(novo);
      chat::Envelope crep;
      if (rpc.call(creq, crep) && crep.has_create_channel_response() && crep.create_channel_response().success())
        channels.push_back(novo);
    }

    // 2) inscrito em menos de 3 canais -> se inscreve em mais um (ao acaso)
    if (sub.count() < 3) {
      std::vector<std::string> candidates;
      for (const auto& c : channels)
        if (!sub.is_subscribed(c)) candidates.push_back(c);
      if (!candidates.empty()) {
        const std::string& c = candidates[pick(candidates.size())];
        sub.subscribe(c);
        println_locked(name + " | inscrito no canal " + c + " (total de inscrições: " +
                       std::to_string(sub.count()) + ")");
        // A inscrição leva um instante para chegar aos publicadores (ZeroMQ "slow joiner");
        // esperamos um pouco para não perder as primeiras mensagens do canal.
        sleep_ms(300);
      }
    }

    // 3) escolhe um canal e publica 10 mensagens, 1 por segundo
    if (channels.empty()) {
      sleep_ms(1000);
      continue;
    }
    const std::string target = channels[pick(channels.size())];
    for (int i = 1; i <= msgs_per_round; ++i) {
      chat::Envelope preq = make_envelope(name);
      auto* p = preq.mutable_publish_request();
      p->set_channel(target);
      p->set_message(phrases[pick(phrases.size())] + " (#" + std::to_string(i) + " de " + name + ")");
      chat::Envelope prep;
      rpc.call(preq, prep);
      sleep_ms(msg_interval_ms);
    }
  }
}