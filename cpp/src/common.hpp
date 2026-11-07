#pragma once
// Utilitários compartilhados por broker, proxy, servidor e cliente (C++).

#include <zmq.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

#include "chat.pb.h"

constexpr size_t kMaxMessageLen = 1000;  // tamanho máximo do texto de uma publicação

// ---------------------------------------------------------------------------
// Relógio físico
//   now_ms()   : hora da máquina, sem correção (usada, por exemplo, para medir a sincronização)
//   clock_ms() : hora usada nas mensagens = hora da máquina + deslocamento calculado pela
//                sincronização com a referência (só os servidores alteram o deslocamento).
//                Nunca retorna um valor menor que o anterior, mesmo se o deslocamento diminuir.
// ---------------------------------------------------------------------------
inline int64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

inline std::atomic<int64_t> g_clock_offset_ms{0};

inline int64_t clock_ms() {
  static std::mutex m;
  static int64_t last = 0;
  int64_t t = now_ms() + g_clock_offset_ms.load();
  std::lock_guard<std::mutex> lock(m);
  if (t < last) t = last;
  last = t;
  return t;
}

// ---------------------------------------------------------------------------
// Relógio lógico (Lamport) - um contador por processo, iniciado junto com ele.
//   tick()    : incrementa ANTES de enviar uma mensagem; o valor vai na mensagem.
//   observe() : ao RECEBER, contador = max(contador, valor recebido). Retorna o novo valor.
// Protegido por mutex porque o bot usa duas threads (REQ e SUB).
// ---------------------------------------------------------------------------
class LogicalClock {
 public:
  int64_t tick() {
    std::lock_guard<std::mutex> lock(m_);
    return ++value_;
  }
  int64_t observe(int64_t received) {
    std::lock_guard<std::mutex> lock(m_);
    if (received > value_) value_ = received;
    return value_;
  }
  int64_t value() const {
    std::lock_guard<std::mutex> lock(m_);
    return value_;
  }

 private:
  mutable std::mutex m_;
  int64_t value_ = 0;
};

inline LogicalClock g_clock;

// Carimba a mensagem imediatamente antes do envio: hora (corrigida) + relógio lógico incrementado.
inline void stamp(chat::Envelope& e) {
  e.set_timestamp_ms(clock_ms());
  e.set_logical_clock(g_clock.tick());
}

// ---------------------------------------------------------------------------

inline std::string fmt_ts(int64_t ms) {
  time_t s = static_cast<time_t>(ms / 1000);
  tm t{};
  gmtime_r(&s, &t);
  char b[32];
  strftime(b, sizeof b, "%Y-%m-%d %H:%M:%S", &t);
  char o[48];
  snprintf(o, sizeof o, "%s.%03dZ", b, static_cast<int>(ms % 1000));
  return o;
}

inline std::string env_or(const char* key, const std::string& fallback) {
  const char* v = std::getenv(key);
  return (v && *v) ? std::string(v) : fallback;
}

// Imprime uma linha inteira de uma vez (o cliente tem mais de uma thread).
inline void println_locked(const std::string& line) {
  static std::mutex m;
  std::lock_guard<std::mutex> lock(m);
  std::cout << line << std::endl;
}

// Cria uma mensagem com o remetente. A hora e o relógio lógico são preenchidos por stamp(),
// logo antes de enviar.
inline chat::Envelope make_envelope(const std::string& sender) {
  chat::Envelope e;
  e.set_timestamp_ms(clock_ms());
  e.set_sender(sender);
  return e;
}

// Nome do usuário/canal: 1..32 caracteres entre [A-Za-z0-9_-]
inline bool valid_name(const std::string& s) {
  if (s.empty() || s.size() > 32) return false;
  for (unsigned char c : s) {
    bool ok = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              c == '_' || c == '-';
    if (!ok) return false;
  }
  return true;
}

// Texto de uma publicação: 1..kMaxMessageLen bytes
inline bool valid_message(const std::string& s) {
  return !s.empty() && s.size() <= kMaxMessageLen;
}

// Texto legível do conteúdo de uma mensagem (para os logs de envio/recebimento)
inline std::string describe(const chat::Envelope& e) {
  std::ostringstream o;
  auto status = [&o](bool ok, chat::ErrorCode code, const std::string& msg) {
    o << "success=" << (ok ? "true" : "false");
    if (!ok) o << ", erro=" << chat::ErrorCode_Name(code) << " (" << msg << ")";
  };
  switch (e.body_case()) {
    case chat::Envelope::kLoginRequest:
      o << "login_request{username=" << e.login_request().username() << "}";
      break;
    case chat::Envelope::kLoginResponse: {
      const auto& r = e.login_response();
      o << "login_response{";
      status(r.success(), r.error(), r.error_message());
      o << "}";
      break;
    }
    case chat::Envelope::kCreateChannelRequest:
      o << "create_channel_request{channel_name=" << e.create_channel_request().channel_name() << "}";
      break;
    case chat::Envelope::kCreateChannelResponse: {
      const auto& r = e.create_channel_response();
      o << "create_channel_response{";
      status(r.success(), r.error(), r.error_message());
      o << "}";
      break;
    }
    case chat::Envelope::kListChannelsRequest:
      o << "list_channels_request{}";
      break;
    case chat::Envelope::kListChannelsResponse: {
      o << "list_channels_response{channels=[";
      const auto& ch = e.list_channels_response().channels();
      for (int i = 0; i < ch.size(); ++i) o << (i ? ", " : "") << ch[i];
      o << "]}";
      break;
    }
    case chat::Envelope::kPublishRequest:
      o << "publish_request{channel=" << e.publish_request().channel() << ", message=\""
        << e.publish_request().message() << "\"}";
      break;
    case chat::Envelope::kPublishResponse: {
      const auto& r = e.publish_response();
      o << "publish_response{";
      status(r.success(), r.error(), r.error_message());
      o << "}";
      break;
    }
    case chat::Envelope::kPublication:
      o << "publication{channel=" << e.publication().channel() << ", author="
        << e.publication().author() << ", message=\"" << e.publication().message() << "\"}";
      break;
    case chat::Envelope::kRankRequest:
      o << "rank_request{name=" << e.rank_request().name() << "}";
      break;
    case chat::Envelope::kRankResponse:
      o << "rank_response{rank=" << e.rank_response().rank() << "}";
      break;
    case chat::Envelope::kListServersRequest:
      o << "list_servers_request{}";
      break;
    case chat::Envelope::kListServersResponse: {
      o << "list_servers_response{servers=[";
      const auto& sv = e.list_servers_response().servers();
      for (int i = 0; i < sv.size(); ++i) o << (i ? ", " : "") << sv[i].name() << ":" << sv[i].rank();
      o << "]}";
      break;
    }
    case chat::Envelope::kHeartbeatRequest:
      o << "heartbeat_request{name=" << e.heartbeat_request().name() << "}";
      break;
    case chat::Envelope::kHeartbeatResponse:
      o << "heartbeat_response{success=" << (e.heartbeat_response().success() ? "true" : "false")
        << ", time_ms=" << e.heartbeat_response().time_ms() << "}";
      break;
    default:
      o << "(mensagem vazia/desconhecida)";
  }
  return o.str();
}

// timestamp | quem | ENVIADO/RECEBIDO | de=... | relogio=<relógio lógico da mensagem> | conteúdo
inline void log_msg(const std::string& who, const char* dir, const chat::Envelope& e) {
  println_locked(fmt_ts(e.timestamp_ms()) + " | " + who + " | " + dir + " | de=" + e.sender() +
                 " | relogio=" + std::to_string(e.logical_clock()) + " | " + describe(e));
}

// Envia a mensagem como está. Chame stamp(e) antes, logo antes do envio.
inline bool send_envelope(zmq::socket_t& sock, const chat::Envelope& e) {
  std::string buf;
  if (!e.SerializeToString(&buf)) return false;
  auto r = sock.send(zmq::buffer(buf), zmq::send_flags::none);
  return r.has_value();
}

// Retorno: 1 = ok, 0 = timeout, -1 = recebeu bytes que não são um Envelope válido.
// Ao receber uma mensagem válida, atualiza o relógio lógico: contador = max(contador, recebido).
inline int recv_envelope(zmq::socket_t& sock, chat::Envelope& e) {
  zmq::message_t m;
  auto r = sock.recv(m, zmq::recv_flags::none);
  if (!r) return 0;
  if (!e.ParseFromArray(m.data(), static_cast<int>(m.size()))) return -1;
  g_clock.observe(e.logical_clock());
  return 1;
}