
# Projeto de Sistemas Distribuídos
Beatriz Cristina Emerenciano RA: 22.222041-0

Larissa Santos Fiuza RA: 22.123.042-8

## 📌 Introdução

Este projeto foi desenvolvido  com o objetivo de implementar um sistema distribuído capaz de realizar comunicação entre múltiplos clientes e servidores utilizando diferentes linguagens de programação.

A arquitetura do sistema é baseada em um modelo intermediado por um broker, responsável por encaminhar as mensagens entre clientes e servidores. O sistema permite operações como login de usuários, criação de canais e listagem de canais, simulando um ambiente básico de comunicação.

Além disso, o projeto foi desenvolvido considerando requisitos obrigatórios, como o uso de serialização binária e a inclusão de timestamp em todas as mensagens trocadas.

---

## 🏗️ Arquitetura do Sistema

O sistema é composto pelos seguintes componentes:

- **Broker**: responsável por intermediar a comunicação entre clientes e servidores (utilizando ZeroMQ).
- **Cliente 1 (Python)**: envia requisições ao sistema.
- **Servidor 1 (Python)**: processa requisições e gerencia dados.
- **Cliente 2 (Java)**: envia requisições utilizando outra linguagem.
- **Servidor 2 (Java)**: recebe e responde requisições em Java.

A comunicação ocorre da seguinte forma:
Cliente → Broker → Servidor → Broker → Cliente


---

## 💻 Tecnologias Utilizadas

### 🔹 Linguagens de Programação
- **Python**: utilizado para implementação do Cliente 1 e Servidor 1.
- **Java**: utilizado para implementação do Cliente 2 e Servidor 2.

A escolha de múltiplas linguagens foi feita para demonstrar interoperabilidade em sistemas distribuídos.

---

### 🔹 Comunicação

- Utilização da biblioteca **ZeroMQ** para comunicação assíncrona entre os componentes.
- Padrão utilizado:
  - `ROUTER` (lado do broker para clientes)
  - `DEALER` (lado do broker para servidores)
  - `REQ/REP` para clientes e servidores

Essa abordagem permite desacoplamento entre os componentes do sistema.

---

### 🔹 Serialização

Foi utilizada a biblioteca **MessagePack** para serialização das mensagens.
As mensagens seguem uma estrutura padronizada contendo:

- `timestamp`: instante de envio da mensagem
- `tipo`: tipo da operação (ex: login, channel, listar_canais)
- `dados`: conteúdo da mensagem (objeto com informações da operação)

Motivos da escolha:
- Formato binário 
- Alta performance
- Compatibilidade entre múltiplas linguagens (Python e Java)

O uso de MessagePack garante interoperabilidade entre diferentes linguagens **sem** depender de formatos textuais como JSON ou XML.

## 🔗 Interoperabilidade

O sistema permite comunicação entre diferentes combinações de clientes e servidores:

- Python → Python  
- Python → Java  
- Java → Python  
- Java → Java  

Isso é possível graças ao uso de:
- Um protocolo comum (MessagePack)
- Um broker intermediador (ZeroMQ)



## ▶️ Como Executar

1. Construir e iniciar os containers:

bash:
Docker compose down
docker compose up --build

## Sistema Distribuído com Req/Rep e Pub/Sub parte 02



O sistema foi desenvolvido utilizando dois padrões de comunicação:

- **Req/Rep (Request/Reply)**  
  Utilizado para operações de controle entre cliente e servidor:
  - Login de usuários
  - Criação de canais
  - Listagem de canais
  - Publicação de mensagens

- **Pub/Sub (Publisher/Subscriber)**  
  Utilizado para distribuição de mensagens em tempo real:
  - O servidor publica mensagens nos canais
  - Os bots (clientes) se inscrevem nos canais e recebem as mensagens



## 🔌 Portas Utilizadas

- **5555 / 5556 → Broker (Req/Rep)**
  - 5555: comunicação com clientes
  - 5556: comunicação com servidores

- **5557 / 5558 → Proxy Pub/Sub**
  - 5557 (XSUB): recebe mensagens do servidor (publisher)
  - 5558 (XPUB): envia mensagens para os bots (subscribers)

---

## 🔄 Fluxo de Comunicação

O fluxo de mensagens ocorre da seguinte forma:

Cliente → Servidor → Proxy Pub/Sub → Bots

1. O cliente envia uma requisição ao servidor (REQ)
2. O servidor processa e publica a mensagem no canal (PUB)
3. O proxy distribui a mensagem para os bots inscritos
4. Os bots recebem e exibem a mensagem

---

## 💾 Persistência de Dados

O sistema armazena informações em arquivos locais:

- `usuarios.txt` → usuários cadastrados
- `canais.txt` → canais criados
- `mensagens.txt` → mensagens publicadas

Esses dados permitem recuperar o histórico do sistema.

---

## 🤖 Funcionamento dos Bots

Os bots foram implementados para automatizar os testes do sistema:

- Criam canais automaticamente caso existam menos de 5
- Se inscrevem em até 3 canais aleatórios
- Executam um loop infinito:
  - Escolhem um canal aleatório
  - Enviam 10 mensagens com intervalo de 1 segundo
- Recebem e exibem mensagens contendo:
  - Canal
  - Mensagem
  - Timestamp de envio
  - Timestamp de recebimento
Essa abordagem permite validar o funcionamento completo do sistema distribuído sem intervenção manual.
---

## ▶️ Como Executar o Projeto

Para rodar o sistema, utilize o Docker:

```bash
docker-compose up --build

As mensagens e dados do sistema são persistidos em arquivos texto simples:

- `usuarios.txt`
- `canais.txt`
- `mensagens.txt`

## ⚙️ Decisões de Projeto

### 🔄 Troca de Mensagens

Para a comunicação entre os componentes do sistema, foram utilizados dois padrões:

- **Req/Rep (Request/Reply)**  
  Escolhido para operações que exigem resposta direta do servidor, como:
  - login
  - criação de canais
  - listagem de canais
  - publicação de mensagens  

  Esse padrão garante que o cliente saiba se a operação foi realizada com sucesso, recebendo uma resposta (OK ou erro).

- **Pub/Sub (Publisher/Subscriber)**  
  Utilizado para distribuição de mensagens em tempo real entre servidor e bots.

  O servidor atua como **publisher**, enviando mensagens para canais (tópicos), enquanto os bots atuam como **subscribers**, recebendo mensagens dos canais nos quais estão inscritos.

  Esse modelo foi escolhido pois permite:
  - desacoplamento entre produtores e consumidores de mensagens
  - escalabilidade (vários bots podem receber mensagens simultaneamente)
  - comunicação assíncrona

---

### 🧱 Uso do Proxy Pub/Sub

Foi utilizado um proxy com sockets **XSUB/XPUB** para intermediar a comunicação entre publishers e subscribers.

Motivações:
- separar a lógica de distribuição de mensagens do servidor
- permitir múltiplos publishers e subscribers
- facilitar a escalabilidade do sistema

---

### 💾 Armazenamento das Publicações

As mensagens e dados do sistema são persistidos em arquivos texto simples:

- `usuarios.txt`
- `canais.txt`
- `mensagens.txt`

A escolha por arquivos `.txt` foi feita por:
- simplicidade de implementação
- facilidade de leitura e depuração
- não necessidade de banco de dados para o escopo do projeto

As mensagens são armazenadas no formato:
>>>>>>> 4284930 (Implementa Parte 2: Pub/Sub com proxy, bot automático e persistência)

## Parte 3 – Relógios e Sincronização

Nesta etapa foram implementados relógios lógicos e sincronização entre os processos do sistema distribuído.

### Relógio Lógico

Foi adicionado em clientes, bots e servidores. O contador é incrementado antes do envio de cada mensagem e atualizado no recebimento utilizando a regra:

```
clock = max(clock_local, clock_recebido) + 1
```

O valor do relógio lógico passou a ser incluído em todas as mensagens no campo `"clock"`.

### Coordenador

Foi criado um novo serviço responsável por:

* Registrar servidores e atribuir um rank
* Manter a lista de servidores ativos
* Remover servidores inativos (sem heartbeat)
* Fornecer o horário para sincronização

### Heartbeat

Os servidores enviam mensagens periódicas ao coordenador (a cada 10 mensagens processadas) para:

* Indicar que continuam ativos
* Atualizar o relógio físico com base no coordenador

### Atualizações realizadas

* `client` e `bot`: implementação de relógio lógico
* `server`: relógio lógico + heartbeat + sincronização
* `coordenador`: novo serviço adicionado
* `docker-compose`: atualizado com o serviço de coordenação

## 🗳️ Eleição de Coordenador

Nesta etapa foi implementado um mecanismo de eleição de coordenador entre os servidores do sistema distribuído.

Cada servidor se registra no coordenador e recebe um **rank único**. A escolha do coordenador é baseada no maior rank disponível na lista de servidores ativos.

Os servidores realizam verificações periódicas utilizando um mecanismo de **heartbeat**, consultando o coordenador para obter a lista atualizada de participantes. Caso o coordenador falhe ou deixe de responder, os servidores detectam a falha e executam automaticamente uma nova eleição.

Esse processo garante a continuidade do sistema, permitindo que outro servidor assuma o papel de coordenador sem interromper o funcionamento da aplicação, assegurando **tolerância a falhas** e maior confiabilidade.

