import zmq
import msgpack
import time
import threading
import json
import os

context = zmq.Context()

# =========================
# PERSISTÊNCIA
# =========================

def carregar_arquivo(nome):

    caminho = f"data/{nome}"

    if os.path.exists(caminho):

        with open(caminho, "r") as f:
            return json.load(f)

    return []


def salvar_arquivo(nome, dados):

    caminho = f"data/{nome}"

    with open(caminho, "w") as f:
        json.dump(dados, f, indent=4)


usuarios = carregar_arquivo("usuarios.json")
canais = carregar_arquivo("canais.json")
mensagens = carregar_arquivo("mensagens.json")

# =========================
# BROKER
# =========================

socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

# =========================
# COORDENADOR
# =========================

coord = context.socket(zmq.REQ)
coord.connect("tcp://coordenador:5560")

# =========================
# PUB/SUB
# =========================

pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy_pubsub:5557")

sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy_pubsub:5558")

sub.setsockopt_string(zmq.SUBSCRIBE, "servers")
sub.setsockopt_string(zmq.SUBSCRIBE, "replica")

# =========================
# VARIÁVEIS
# =========================

nome = f"servidor-py-{time.time()}"
rank = 0
coordenador = None

clock = 0
contador = 0

historico = []

INTERVALO = 2

# =========================
# REGISTRO
# =========================

def registrar():

    global rank

    msg = {
        "tipo": "register",
        "dados": {
            "nome": nome
        }
    }

    coord.send(msgpack.packb(msg))

    resp = msgpack.unpackb(
        coord.recv(),
        raw=False
    )

    rank = resp["dados"]["rank"]

    print("Registrado rank:", rank)


# =========================
# PEDIR SERVIDORES
# =========================

def pedir_servidores():

    coord.send(
        msgpack.packb({
            "tipo": "get_servers"
        })
    )

    return msgpack.unpackb(
        coord.recv(),
        raw=False
    )


# =========================
# ELEIÇÃO
# =========================

def eleger(lista):

    maior = -1
    eleito = None

    for s in lista["dados"]:

        if s["rank"] > maior:

            maior = s["rank"]
            eleito = s["nome"]

    return eleito


# =========================
# PUBLICAR COORDENADOR
# =========================

def publicar_coord(nome_coord):

    payload = {
        "tipo": "coordenador",
        "coordenador": nome_coord
    }

    pub.send_string(
        "servers " +
        msgpack.packb(payload).hex()
    )


# =========================
# OUVIR PUBSUB
# =========================

def ouvir_pubsub():

    global coordenador

    while True:

        try:

            msg = sub.recv_string()

            canal, payload_hex = msg.split(" ", 1)

            payload = msgpack.unpackb(
                bytes.fromhex(payload_hex),
                raw=False
            )

            if canal == "servers":

                novo = payload["coordenador"]

                if novo != coordenador:

                    coordenador = novo

                    print(
                        "Novo coordenador:",
                        coordenador
                    )

            elif canal == "replica":

                historico.append(
                    payload["dados"]
                )

                print(
                    "Replica recebida:",
                    payload["dados"]
                )

        except Exception as e:

            print("Erro SUB:", e)


# =========================
# HEARTBEAT
# =========================

def heartbeat():

    global coordenador

    ultimo = None

    while True:

        try:

            coord.send(
                msgpack.packb({
                    "tipo": "heartbeat",
                    "dados": {
                        "nome": nome
                    }
                })
            )

            coord.recv()

            lista = pedir_servidores()

            novo = eleger(lista)

            if novo != coordenador:

                coordenador = novo

                publicar_coord(novo)

                print(
                    "Novo coord eleito:",
                    novo
                )

            if ultimo != coordenador:

                print(
                    "Coordenador atual:",
                    coordenador
                )

                ultimo = coordenador

        except:

            print("Erro heartbeat")

        time.sleep(INTERVALO)


# =========================
# BERKELEY
# =========================

def sync_clock():

    try:

        coord.send(
            msgpack.packb({
                "tipo": "sync_clock"
            })
        )

        resp = msgpack.unpackb(
            coord.recv(),
            raw=False
        )

        print("Berkeley sync:", resp)

    except:

        print("Erro Berkeley sync")


# =========================
# INICIALIZAÇÃO
# =========================

registrar()

lista = pedir_servidores()

coordenador = eleger(lista)

print("Coord inicial:", coordenador)

threading.Thread(
    target=heartbeat,
    daemon=True
).start()

threading.Thread(
    target=ouvir_pubsub,
    daemon=True
).start()

# =========================
# LOOP PRINCIPAL
# =========================

while True:

    try:

        msg = msgpack.unpackb(
            socket.recv(),
            raw=False
        )

        tipo = msg.get("tipo")

        dados = msg.get("dados", {})

        clock_recebido = msg.get(
            "clock",
            0
        )

        clock = max(
            clock,
            clock_recebido
        ) + 1

        contador += 1

        # =========================
        # LOGIN
        # =========================

        if tipo == "login":

            usuario = dados["usuario"]

            existe = False

            for u in usuarios:

                if u["usuario"] == usuario:

                    existe = True

            if not existe:

                usuarios.append({
                    "usuario": usuario,
                    "timestamp": time.time()
                })

                salvar_arquivo(
                    "usuarios.json",
                    usuarios
                )

                print(
                    "Usuário salvo:",
                    usuario
                )

            resposta = {
                "tipo": "login_ok",
                "clock": clock
            }

            socket.send(
                msgpack.packb(resposta)
            )

            continue

        # =========================
        # CHANNEL
        # =========================

        if tipo == "channel":

            nome_canal = dados["nome"]

            if nome_canal not in canais:

                canais.append(nome_canal)

                salvar_arquivo(
                    "canais.json",
                    canais
                )

                print(
                    "Canal criado:",
                    nome_canal
                )

            resposta = {
                "tipo": "channel_ok",
                "clock": clock
            }

            socket.send(
                msgpack.packb(resposta)
            )

            continue

        # =========================
        # LIST CHANNELS
        # =========================

        if tipo == "list_channels":

            resposta = {
                "tipo": "channels_list",
                "dados": canais,
                "clock": clock
            }

            socket.send(
                msgpack.packb(resposta)
            )

            continue

        # =========================
        # PUBLICAÇÃO
        # =========================

        if tipo == "publish":

            mensagens.append({
                "canal": dados["canal"],
                "mensagem": dados["mensagem"],
                "timestamp": time.time(),
                "clock": clock
            })

            salvar_arquivo(
                "mensagens.json",
                mensagens
            )

            payload = {
                "mensagem": dados["mensagem"],
                "timestamp_envio": time.time(),
                "clock": clock
            }

            pub.send_string(
                dados["canal"] +
                " " +
                msgpack.packb(payload).hex()
            )

            print(
                "Mensagem publicada:",
                dados["mensagem"]
            )

        # =========================
        # REPLICAÇÃO
        # =========================

        registro = {
            "server": nome,
            "msg": msg,
            "clock": clock
        }

        historico.append(registro)

        pub.send_string(
            "replica " +
            msgpack.packb({
                "tipo": "replica",
                "dados": registro
            }).hex()
        )

        # =========================
        # BERKELEY
        # =========================

        if contador >= 15:

            sync_clock()

            contador = 0

        # =========================
        # RESPOSTA
        # =========================

        resposta = {
            "clock": clock,
            "tipo": "resposta",
            "dados": "OK Python"
        }

        socket.send(
            msgpack.packb(resposta)
        )

    except Exception as e:

        print("Erro server:", e)