FROM maven:3.9-eclipse-temurin-17 AS builder
RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /src
# Copia o diretório proto compartilhado e o diretório java
COPY proto ./proto
COPY java ./java

WORKDIR /src/java
# Compila o arquivo .proto e gera os arquivos Java dentro do src
RUN mkdir -p src/main/java \
    && protoc --proto_path=../proto --java_out=src/main/java ../proto/chat.proto \
    && mvn clean package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /src/java/target/chat-java-1.0-jar-with-dependencies.jar app.jar