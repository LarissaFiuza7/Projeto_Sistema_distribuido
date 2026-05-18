import zmq
import msgpack
import time
import random

clock = 0

def criar_mensagem(tipo, dados):
    global clock
    clock += 1
    return msgpack.packb({
        "timestamp": time.time(),
        "clock": clock,
        "tipo": tipo,
        "dados": dados
    })

def atualizar_clock(resposta):
    global clock
    clock = max(clock, resposta.get("clock", 0)) + 1

context = zmq.Context()

socket_req = context.socket(zmq.REQ)
socket_req.connect("tcp://broker:5555")

socket_sub = context.socket(zmq.SUB)
socket_sub.connect("tcp://proxy_pubsub:5558")

socket_req.send(criar_mensagem("login", {"usuario": "Beatriz"}))
resposta = msgpack.unpackb(socket_req.recv(), raw=False)
atualizar_clock(resposta)
print("Login efetuado. Resposta:", resposta)
print("-" * 40)

socket_req.send(criar_mensagem("listar canais", {}))
resposta = msgpack.unpackb(socket_req.recv(), raw=False)
atualizar_clock(resposta)

canais_existentes = resposta.get("dados", [])
print("Canais disponíveis no servidor:", canais_existentes)

if len(canais_existentes) < 5:
    novo_canal = f"canal_{int(time.time())}"
    print(f"Menos de 5 canais encontrados. Criando o canal: {novo_canal}")
    socket_req.send(criar_mensagem("channel", {"nome": novo_canal}))
    resposta = msgpack.unpackb(socket_req.recv(), raw=False)
    atualizar_clock(resposta)
    if novo_canal not in canais_existentes:
        canais_existentes.append(novo_canal)

canais_para_inscrever = canais_existentes[:3]
for canal in canais_para_inscrever:
    print(f"Se inscrevendo no canal (tópico): {canal}")
    socket_sub.setsockopt_string(zmq.SUBSCRIBE, canal)

print("-" * 40)

print("Iniciando loop infinito de envio de mensagens...")
while True:
    canal_escolhido = random.choice(canais_existentes) if canais_existentes else "geral"
    
    print(f"\n>>> Iniciando bloco de 10 mensagens no canal: [{canal_escolhido}]")
    
    for i in range(10):
        dados_publicacao = {
            "canal": canal_escolhido,
            "mensagem": f"Mensagem automatizada {i} de Beatriz"
        }
        
        socket_req.send(criar_mensagem("publish", dados_publicacao))
        
        resposta = msgpack.unpackb(socket_req.recv(), raw=False)
        atualizar_clock(resposta)
        
        print(f"[Clock Local: {clock}] Confirmação do Servidor: {resposta}")
        
        time.sleep(1.0)
    
    time.sleep(2.0)