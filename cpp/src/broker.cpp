// Broker: faz a ponte entre os clientes (REQ) e os servidores (REP).
//   clientes  -> tcp://*:5555 (ROUTER)
//   servidores-> tcp://*:5556 (DEALER)
// O DEALER distribui as requisições entre os servidores conectados (round-robin),
// o que permite adicionar/remover servidores sem parar o serviço.
#include <zmq.hpp>

#include <iostream>

#include "common.hpp"

int main() {
  const std::string fe_addr = env_or("BROKER_FRONTEND_BIND", "tcp://*:5555");
  const std::string be_addr = env_or("BROKER_BACKEND_BIND", "tcp://*:5556");

  zmq::context_t ctx(1);
  zmq::socket_t frontend(ctx, zmq::socket_type::router);
  zmq::socket_t backend(ctx, zmq::socket_type::dealer);
  frontend.bind(fe_addr);
  backend.bind(be_addr);

  std::cout << "[broker] frontend (clientes) em " << fe_addr << " | backend (servidores) em "
            << be_addr << std::endl;

  zmq::proxy(frontend, backend);
  return 0;
}