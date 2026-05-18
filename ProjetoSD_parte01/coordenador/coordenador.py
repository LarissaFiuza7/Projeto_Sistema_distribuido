import zmq
import msgpack
import time

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5560")

print("Serviço de Referência iniciado...")

servidores = {}
rank_counter = 1
coordenador_atual = None  
TIMEOUT = 10  

historico_global = []

def adicionar_historico(evento):
    registro = {
        "timestamp": time.time(),
        "evento": evento
    }
    historico_global.append(registro)
    if "Heartbeat" not in evento:
        print("[HISTÓRICO]", registro)

def limpar_servidores():
    global coordenador_atual
    agora = time.time()
    remover = []

    for nome, info in servidores.items():
        tempo_sem_heartbeat = agora - info["last_seen"]
        if tempo_sem_heartbeat > TIMEOUT:
            remover.append(nome)

    for nome in remover:
        adicionar_historico(f"Servidor removido por timeout: {nome}")
        del servidores[nome]

    if coordenador_atual not in servidores and servidores:
        coordenador_atual = max(servidores.items(), key=lambda x: x[1]["rank"])[0]
        adicionar_historico(f"Coordenador antigo caiu. Novo eleito por rank: {coordenador_atual}")
    elif not servidores:
        coordenador_atual = None

while True:
    limpar_servidores()

    try:
        msg = socket.recv(flags=zmq.NOBLOCK)
    except zmq.Again:
        time.sleep(0.5)
        continue

    data = msgpack.unpackb(msg, raw=False)
    tipo = data.get("tipo")
    dados = data.get("dados", {})

    if tipo == "register":
        nome = dados["nome"]
        if nome not in servidores:
            servidores[nome] = {
                "rank": rank_counter,
                "last_seen": time.time()
            }
            adicionar_historico(f"Servidor cadastrado: {nome} (rank {rank_counter})")
            rank_counter += 1

        if coordenador_atual is None or servidores[nome]["rank"] > servidores[coordenador_atual]["rank"]:
            coordenador_atual = nome
            adicionar_historico(f"Novo líder definido por rank: {coordenador_atual}")

        resposta = {
            "tipo": "register_ok",
            "dados": {
                "rank": servidores[nome]["rank"],
                "coordenador": coordenador_atual
            }
        }

    elif tipo == "get_servers":
        lista = [{"nome": n, "rank": i["rank"]} for n, i in servidores.items()]
        resposta = {"tipo": "servers_list", "dados": lista}

    elif tipo == "heartbeat":
        nome = dados["nome"]
        if nome in servidores:
            servidores[nome]["last_seen"] = time.time()
        
        if coordenador_atual not in servidores and servidores:
            coordenador_atual = max(servidores.items(), key=lambda x: x[1]["rank"])[0]
        elif not servidores:
            coordenador_atual = None

        resposta = {
            "tipo": "heartbeat_ok", 
            "dados": {
                "coordenador": coordenador_atual
            }
        }

    elif tipo == "get_history":
        resposta = {"tipo": "history", "dados": historico_global}

    else:
        resposta = {"tipo": "erro", "dados": "comando inválido"}

    socket.send(msgpack.packb(resposta, use_bin_type=True))