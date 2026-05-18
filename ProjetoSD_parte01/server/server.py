import zmq
import msgpack
import time
import threading
import json
import os

context = zmq.Context()

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

socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

coord = context.socket(zmq.REQ)
coord.connect("tcp://coordenador:5560")

pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy_pubsub:5557")

sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy_pubsub:5558")

sub.setsockopt_string(zmq.SUBSCRIBE, "servers")
sub.setsockopt_string(zmq.SUBSCRIBE, "replica")
sub.setsockopt_string(zmq.SUBSCRIBE, "berkeley")

nome = f"servidor-py-{time.time()}"
rank = 0
coordenador = None

clock_logico = 0
relogio_fisico_local = int(time.time())
contador = 0

historico = []
tempos_recebidos = {}

def registrar():
    global rank
    msg = {
        "tipo": "register",
        "dados": {
            "nome": nome
        }
    }
    coord.send(msgpack.packb(msg))
    resp = msgpack.unpackb(coord.recv(), raw=False)
    rank = resp["dados"]["rank"]
    print("Registrado rank:", rank)

def pedir_servidores():
    coord.send(msgpack.packb({"tipo": "get_servers"}))
    return msgpack.unpackb(coord.recv(), raw=False)

def eleger(lista):
    maior = -1
    eleito = None
    for s in lista["dados"]:
        if s["rank"] > maior:
            maior = s["rank"]
            eleito = s["nome"]
    return eleito

def publicar_coord(nome_coord):
    payload = {
        "tipo": "coordenador",
        "coordenador": nome_coord
    }
    pub.send_string("servers " + msgpack.packb(payload).hex())

def ouvir_pubsub():
    global coordenador, relogio_fisico_local, tempos_recebidos
    while True:
        try:
            msg = sub.recv_string()
            canal, payload_hex = msg.split(" ", 1)
            payload = msgpack.unpackb(bytes.fromhex(payload_hex), raw=False)
            
            if canal == "servers":
                novo = payload["coordenador"]
                if novo != coordenador:
                    coordenador = novo
                    print("Novo coordenador:", coordenador)
            
            elif canal == "replica":
                historico.append(payload["dados"])
                print("Replica recebida:", payload["dados"])
                
            elif canal == "berkeley":
                tipo_b = payload.get("tipo")
                sender = payload.get("remetente")
                
                if tipo_b == "req_tempo" and nome == coordenador:
                    pass
                elif tipo_b == "req_tempo" and nome != coordenador:
                    resp_tempo = {
                        "tipo": "resp_tempo",
                        "remetente": nome,
                        "tempo": relogio_fisico_local
                    }
                    pub.send_string("berkeley " + msgpack.packb(resp_tempo).hex())
                    
                elif tipo_b == "resp_tempo" and nome == coordenador:
                    tempos_recebidos[sender] = payload.get("tempo")
                    
                elif tipo_b == "ajuste_tempo":
                    ajustes = payload.get("ajustes", {})
                    if nome in ajustes:
                        relogio_fisico_local += adjustments = ajustes[nome]
                        print(f"[Berkeley] Relógio físico ajustado em {ajustes[nome]}s. Novo tempo: {relogio_fisico_local}")
                        
        except Exception as e:
            print("Erro SUB:", e)

def executar_berkeley():
    global tempos_recebidos, relogio_fisico_local
    if coordenador != nome:
        return
    
    print("[Berkeley] Iniciando checagem de relógios como Coordenador...")
    tempos_recebidos = {nome: relogio_fisico_local}
    
    req_msg = {"tipo": "req_tempo", "remetente": nome}
    pub.send_string("berkeley " + msgpack.packb(req_msg).hex())
    
    time.sleep(1.5)
    
    if len(tempos_recebidos) <= 1:
        return
        
    soma_tempos = sum(tempos_recebidos.values())
    media_tempo = soma_tempos // len(tempos_recebidos)
    
    ajustes = {}
    for srv, tempo_srv in tempos_recebidos.items():
        ajustes[srv] = int(media_tempo - tempo_srv)
        
    ajuste_msg = {
        "tipo": "ajuste_tempo",
        "ajustes": ajustes
    }
    pub.send_string("berkeley " + msgpack.packb(ajuste_msg).hex())
    tempos_recebidos.clear()

registrar()
lista = pedir_servidores()
coordenador = eleger(lista)
print("Coord inicial:", coordenador)

threading.Thread(target=ouvir_pubsub, daemon=True).start()

def tic_tac():
    global relogio_fisico_local
    while True:
        time.sleep(1)
        relogio_fisico_local += 1
threading.Thread(target=tic_tac, daemon=True).start()

while True:
    try:
        msg = msgpack.unpackb(socket.recv(), raw=False)

        tipo = msg.get("tipo")
        dados = msg.get("dados", {})
        clock_recebido = msg.get("clock", 0)

        clock_logico = max(clock_logico, clock_recebido) + 1
        contador += 1

        resposta = None

        if tipo == "login":
            usuario = dados["usuario"]
            existe = False
            for u in usuarios:
                if u["usuario"] == usuario:
                    existe = True
            if not existe:
                usuarios.append({
                    "usuario": usuario,
                    "timestamp": relogio_fisico_local
                })
                salvar_arquivo("usuarios.json", usuarios)
                print("Usuário salvo:", usuario)
            
            resposta = {"tipo": "login_ok", "clock": clock_logico}

        elif tipo == "channel":
            nome_canal = dados["nome"]
            if nome_canal not in canais:
                canais.append(nome_canal)
                salvar_arquivo("canais.json", canais)
                print("Canal criado:", nome_canal)
            
            resposta = {"tipo": "channel_ok", "clock": clock_logico}

        elif tipo == "list_channels":
            resposta = {
                "tipo": "channels_list",
                "dados": canais,
                "clock": clock_logico
            }

        elif tipo == "publish":
            mensagens.append({
                "canal": dados["canal"],
                "mensagem": dados["mensagem"],
                "timestamp": relogio_fisico_local,
                "clock": clock_logico
            })
            salvar_arquivo("mensagens.json", mensagens)

            payload = {
                "mensagem": dados["mensagem"],
                "timestamp_envio": relogio_fisico_local,
                "clock": clock_logico
            }

            pub.send_string(dados["canal"] + " " + msgpack.packb(payload).hex())
            print("Mensagem publicada:", dados["mensagem"])
            
            resposta = {
                "clock": clock_logico,
                "tipo": "resposta",
                "dados": "OK Python"
            }

        else:
            resposta = {"tipo": "erro", "dados": "comando desconhecido", "clock": clock_logico}

        # Envia a resposta estrita ao broker (REP) antes de qualquer outra comunicação de rede
        socket.send(msgpack.packb(resposta))

        # Realiza a replicação passiva/ativa em segundo plano
        registro = {
            "server": nome,
            "msg": msg,
            "clock": clock_logico
        }
        historico.append(registro)
        pub.send_string("replica " + msgpack.packb({"tipo": "replica", "dados": registro}).hex())

        # Gerenciamento seguro dos gatilhos de Heartbeat e Berkeley
        if contador >= 10:
            try:
                coord.send(msgpack.packb({"tipo": "heartbeat", "dados": {"nome": nome}}))
                coord.recv()
                lista_srv = pedir_servidores()
                novo_coord = eleger(lista_srv)
                if novo_coord and novo_coord != coordenador:
                    coordenador = novo_coord
                    publicar_coord(novo_coord)
            except Exception as e_coord:
                print("Aviso: Coordenador de referência indisponível para heartbeat:", e_coord)

            if contador >= 15:
                executar_berkeley()
                contador = 0

    except Exception as e:
        print("Erro server:", e)