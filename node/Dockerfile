# Imagem única para broker, servidor e cliente Node/TypeScript.
# Build context = raiz do repositório (precisa enxergar proto/ e node/).
FROM node:20

WORKDIR /app

# Dependências primeiro (aproveita cache do Docker)
COPY node/package*.json ./
RUN npm ci

COPY proto ./proto
COPY node/tsconfig.json ./
COPY node/src ./src

ENV PROTO_PATH=/app/proto/chat.proto

# Cada serviço do compose sobrescreve o command
CMD ["npx", "tsx", "src/server.ts"]