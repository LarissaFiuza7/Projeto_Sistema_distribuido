import zmq
import msgpack
import time

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5560")

print("Coordenador iniciado...")

servidores = {}
rank_counter = 1

TIMEOUT = 60

def limpar_servidores():
    agora = time.time()
    remover = []

    for nome, info in servidores.items():
        if agora - info["last_seen"] > TIMEOUT:
            remover.append(nome)

    for nome in remover:
        print(f"Removendo servidor inativo: {nome}")
        del servidores[nome]

while True:
    msg = socket.recv()
    data = msgpack.unpackb(msg, raw=False)

    tipo = data.get("tipo")
    dados = data.get("dados", {})

    limpar_servidores()

    # 🔹 REGISTER
    if tipo == "register":
        nome = dados["nome"]

        if nome not in servidores:
            servidores[nome] = {
                "rank": rank_counter,
                "last_seen": time.time()
            }
            rank_counter += 1

        resposta = {
            "tipo": "register_ok",
            "dados": {
                "rank": servidores[nome]["rank"]
            }
        }

    # 🔹 LISTA SERVIDORES
    elif tipo == "get_servers":
        lista = [
            {"nome": nome, "rank": info["rank"]}
            for nome, info in servidores.items()
        ]

        resposta = {
            "tipo": "servers_list",
            "dados": lista
        }

    # 🔹 HEARTBEAT (AGORA SEM HORA)
    elif tipo == "heartbeat":
        nome = dados["nome"]

        if nome in servidores:
            servidores[nome]["last_seen"] = time.time()

        resposta = {
            "tipo": "heartbeat_ok"
        }

    # 🔹 ERRO
    else:
        resposta = {
            "tipo": "erro",
            "dados": "comando inválido"
        }

    socket.send(msgpack.packb(resposta))