import zmq
import msgpack
import time

# =========================
# CONFIGURAÇÃO ZMQ
# =========================
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5560")

print("Coordenador iniciado...")

# =========================
# ESTRUTURAS GLOBAIS
# =========================
servidores = {}
rank_counter = 1
coordenador_atual = None  # Guarda o nome do coordenador
TIMEOUT = 10  # Timeout para detectar servidor morto

# =========================
# HISTÓRICO GLOBAL
# =========================
historico_global = []

def adicionar_historico(evento):
    registro = {
        "timestamp": time.time(),
        "evento": evento
    }
    historico_global.append(registro)

    # NÃO printa heartbeat
    if "Heartbeat" not in evento:
        print("[HISTÓRICO]", registro)

# =========================
# LIMPEZA DE SERVIDORES
# =========================
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

    # Verifica se o coordenador morreu
    if coordenador_atual not in servidores and servidores:
        # Eleição: o servidor com maior rank assume
        coordenador_atual = max(servidores.items(), key=lambda x: x[1]["rank"])[0]
        adicionar_historico(f"Coordenador morreu. Novo coordenador: {coordenador_atual}")
    elif not servidores:
        coordenador_atual = None

# =========================
# LOOP PRINCIPAL
# =========================
while True:
    limpar_servidores()

    try:
        msg = socket.recv(flags=zmq.NOBLOCK)
    except zmq.Again:
        time.sleep(1)
        continue

    # =========================
    # PROCESSA MENSAGEM
    # =========================
    data = msgpack.unpackb(msg, raw=False)
    tipo = data.get("tipo")
    dados = data.get("dados", {})

    # =========================
    # REGISTER
    # =========================
    if tipo == "register":
        nome = dados["nome"]

        if nome not in servidores:
            servidores[nome] = {
                "rank": rank_counter,
                "last_seen": time.time()
            }
            adicionar_historico(f"Servidor registrado: {nome} (rank {rank_counter})")
            rank_counter += 1

        # Define coordenador se ainda não tiver
        if coordenador_atual is None or servidores[nome]["rank"] > servidores[coordenador_atual]["rank"]:
            coordenador_atual = nome
            adicionar_historico(f"Novo coordenador: {coordenador_atual}")

        resposta = {
            "tipo": "register_ok",
            "dados": {
                "rank": servidores[nome]["rank"],
                "coordenador": coordenador_atual
            }
        }

    # =========================
    # GET SERVERS
    # =========================
    elif tipo == "get_servers":
        lista = [{"nome": n, "rank": i["rank"]} for n, i in servidores.items()]
        resposta = {"tipo": "servers_list", "dados": lista}

    # =========================
    # HEARTBEAT
    # =========================
    elif tipo == "heartbeat":
        nome = dados["nome"]
        if nome in servidores:
            servidores[nome]["last_seen"] = time.time()
            adicionar_historico(f"Heartbeat recebido de {nome}")

        # Se coordenador morreu, eleição
        if coordenador_atual not in servidores and servidores:
            coordenador_atual = max(servidores.items(), key=lambda x: x[1]["rank"])[0]
            adicionar_historico(f"Coordenador morreu. Novo coordenador: {coordenador_atual}")
        elif not servidores:
            coordenador_atual = None

        resposta = {"tipo": "heartbeat_ok", "dados": {"coordenador": coordenador_atual}}

    # =========================
    # SINCRONIZAÇÃO BERKELEY
    # =========================
    elif tipo == "sync_clock":
        nome = dados.get("nome")
        hora_local = int(time.time())

        if nome == coordenador_atual:
            offsets = {}
            for srv, info in servidores.items():
                if srv != coordenador_atual:
                    hora_srv = info.get("clock", hora_local)
                    offsets[srv] = hora_srv - hora_local
            media_offset = sum(offsets.values()) // len(offsets) if offsets else 0
            resposta = {"tipo": "clock_sync", "dados": {"ajuste": media_offset}}
        else:
            ajuste = data.get("dados", {}).get("ajuste", 0)
            hora_local += ajuste
            resposta = {"tipo": "clock_sync_ok", "dados": {"hora": hora_local}}

    # =========================
    # HISTÓRICO
    # =========================
    elif tipo == "get_history":
        resposta = {"tipo": "history", "dados": historico_global}

    # =========================
    # COMANDO INVÁLIDO
    # =========================
    else:
        resposta = {"tipo": "erro", "dados": "comando inválido"}

    # =========================
    # ENVIA RESPOSTA
    # =========================
    socket.send(msgpack.packb(resposta, use_bin_type=True))