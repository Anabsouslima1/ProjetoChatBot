// Proxy Pub/Sub (separado do broker Req/Rep).
//   servidores (PUB) conectam em XSUB -> tcp://*:5557
//   bots       (SUB) conectam em XPUB -> tcp://*:5558
// O proxy repassa as publicações aos assinantes e as inscrições (tópicos) aos publicadores.
// Cada publicação tem 2 frames: [tópico = nome do canal][Envelope serializado com a Publication].
#include <zmq.hpp>

#include <iostream>

#include "common.hpp"

int main() {
  const std::string xsub_addr = env_or("PROXY_XSUB_BIND", "tcp://*:5557");
  const std::string xpub_addr = env_or("PROXY_XPUB_BIND", "tcp://*:5558");

  zmq::context_t ctx(1);
  zmq::socket_t xsub(ctx, zmq::socket_type::xsub);
  zmq::socket_t xpub(ctx, zmq::socket_type::xpub);
  xsub.bind(xsub_addr);
  xpub.bind(xpub_addr);

  std::cout << "[proxy] XSUB (servidores/publicadores) em " << xsub_addr
            << " | XPUB (bots/assinantes) em " << xpub_addr << std::endl;

  zmq::proxy(xsub, xpub);
  return 0;
}
