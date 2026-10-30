# Projeto ChatBot Distribuído

Sistema de chat distribuído baseado em microsserviços, desenvolvido com **ZeroMQ**, **Protocol Buffers (Protobuf)** e persistência de dados em ambiente Docker, com suporte a comunicação multi-linguagem (Java e C++).

## 🚀 Arquitetura do Sistema

O projeto é composto por diferentes módulos integrados:
- **Broker (C++):** Responsável pelo encaminhamento centralizado de mensagens utilizando o padrão REQ/REP.
- **Proxy Pub/Sub (Java):** Gerencia a distribuição assíncrona de mensagens para os canais de interesse (tópicos).
- **Servidores e Clientes/Bots (Java e C++):** Executam o ciclo de vida de autenticação (login), gestão de canais e publicação/subscrição de mensagens.

---

## 📂 Estrutura do Repositório

```text
├── cpp/                  # Código-fonte dos servidores e clientes em C++
├── java/                 # Código-fonte dos clientes, servidores e proxies em Java
├── proto/                # Contrato compartilhado de mensagens (chat.proto)
├── data/                 # Volumes montados para persistência de dados
├── docker-compose.yaml   # Orquestração principal dos contentores
└── Dockerfile            # Configurações de build para os serviços
