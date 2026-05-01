import zmq
import msgpack
import time

#  RELÓGIO LÓGICO
clock = 0

def atualizar_clock(recebido):
    global clock
    clock = max(clock, recebido) + 1


# ====== CARREGAR DADOS ======
def carregar_usuarios():
    try:
        with open("usuarios.txt", "r") as f:
            return [linha.split(",")[0] for linha in f.readlines()]
    except FileNotFoundError:
        return []

def carregar_canais():
    try:
        with open("canais.txt", "r") as f:
            return [linha.strip() for linha in f.readlines()]
    except FileNotFoundError:
        return []

usuarios = carregar_usuarios()
canais = carregar_canais()


# ====== SALVAR DADOS ======
def salvar_login(usuario):
    with open("usuarios.txt", "a") as f:
        f.write(f"{usuario},{time.time()}\n")

def salvar_canal(canal):
    with open("canais.txt", "a") as f:
        f.write(f"{canal}\n")


# ====== RESPOSTA COM CLOCK ======
def criar_resposta(tipo, dados):
    global clock
    return msgpack.packb({
        "timestamp": time.time(),
        "clock": clock,
        "tipo": tipo,
        "dados": dados
    })


# ====== ZMQ ======
context = zmq.Context()

# comunicação com broker
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

# PUB/SUB
pub_socket = context.socket(zmq.PUB)
pub_socket.connect("tcp://proxy_pubsub:5557")

#  conexão com coordenador
coord = context.socket(zmq.REQ)
coord.connect("tcp://coordenador:5560")
nome_servidor = f"servidor-py-{time.time()}"
#  dados do servidor
nome_servidor = f"servidor-py-{time.time()}"
meu_rank = None
coordenador_atual = None
contador = 0

print("Servidor Python rodando...")


# ====== COORDENADOR ======

def registrar():
    global meu_rank

    msg = msgpack.packb({
        "tipo": "register",
        "dados": {"nome": nome_servidor}
    })

    coord.send(msg)
    resp = msgpack.unpackb(coord.recv(), raw=False)

    meu_rank = resp["dados"]["rank"]
    print("Registrado no coordenador | Rank:", meu_rank)


def get_servidores():
    coord.send(msgpack.packb({"tipo": "get_servers"}))
    resp = msgpack.unpackb(coord.recv(), raw=False)
    return resp["dados"]


def eleger(lista):
    maior = -1
    eleito = None

    for s in lista:
        if s["rank"] > maior:
            maior = s["rank"]
            eleito = s["nome"]

    return eleito


def verificar_coordenador():
    global coordenador_atual

    # heartbeat
    coord.send(msgpack.packb({
        "tipo": "heartbeat",
        "dados": {"nome": nome_servidor}
    }))
    coord.recv()

    lista = get_servidores()
    print("Lista servidores:", lista)

    novo = eleger(lista)

    if coordenador_atual != novo:
        print("Novo coordenador eleito:", novo)
        coordenador_atual = novo


#  inicialização
registrar()

lista = get_servidores()
coordenador_atual = eleger(lista)

print("Coordenador inicial:", coordenador_atual)


# ====== LOOP PRINCIPAL ======
while True:
    mensagem = msgpack.unpackb(socket.recv(), raw=False)

    #  ATUALIZA CLOCK AO RECEBER
    clock_recebido = mensagem.get("clock", 0)
    atualizar_clock(clock_recebido)

    print("Clock servidor (recebeu):", clock)

    tipo = mensagem["tipo"]
    dados = mensagem["dados"]

    #  INCREMENTA ANTES DE RESPONDER
    clock += 1

    contador += 1

    #  VERIFICA COORDENADOR A CADA 10 MSG
    if contador % 10 == 0:
        verificar_coordenador()

    # ===== LOGIN =====
    if tipo == "login":
        usuario = dados["usuario"]

        if usuario not in usuarios:
            usuarios.append(usuario)
            salvar_login(usuario)

            resposta = criar_resposta("login", {
                "status": "sucesso",
                "mensagem": "Login realizado"
            })
        else:
            resposta = criar_resposta("login", {
                "status": "erro",
                "mensagem": "Usuário já existe"
            })

    # ===== PUBLISH =====
    elif tipo == "publish":
        canal = dados["canal"]
        mensagem_texto = dados["mensagem"]

        if canal not in canais:
            resposta = criar_resposta("publish", {
                "status": "erro",
                "mensagem": "Canal não existe"
            })
        else:
            payload = {
                "canal": canal,
                "mensagem": mensagem_texto,
                "timestamp_envio": time.time(),
                "clock": clock
            }

            pub_socket.send_string(f"{canal} {msgpack.packb(payload).hex()}")

            with open("mensagens.txt", "a") as f:
                f.write(f"{canal}|{mensagem_texto}|{payload['timestamp_envio']}\n")

            resposta = criar_resposta("publish", {
                "status": "sucesso",
                "mensagem": "Mensagem publicada"
            })

    # ===== CANAL =====
    elif tipo == "channel":
        canal = dados["nome"]

        if canal not in canais:
            canais.append(canal)
            salvar_canal(canal)

            resposta = criar_resposta("channel", {
                "status": "sucesso",
                "mensagem": "Canal criado"
            })
        else:
            resposta = criar_resposta("channel", {
                "status": "erro",
                "mensagem": "Canal já existe"
            })

    # ===== LISTAR CANAIS =====
    elif tipo == "listar_canais":
        resposta = criar_resposta("listar_canais", {
            "canais": canais
        })

    # ===== ERRO =====
    else:
        resposta = criar_resposta("erro", {
            "mensagem": "Comando inválido"
        })

    socket.send(resposta)