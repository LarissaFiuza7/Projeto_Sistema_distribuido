import zmq
import msgpack
import time

context = zmq.Context()

socket = context.socket(zmq.SUB)
socket.connect("tcp://proxy_pubsub:5558")

#  ESCUTA TODOS CANAIS
socket.setsockopt_string(zmq.SUBSCRIBE, "")

print("Bot iniciado...")

clock = 0

def atualizar_clock(recebido):
    global clock
    clock = max(clock, recebido) + 1


while True:
    try:
        msg = socket.recv_string(flags=zmq.NOBLOCK)

        canal, payload_hex = msg.split(" ", 1)
        payload = msgpack.unpackb(bytes.fromhex(payload_hex), raw=False)

        clock_recebido = payload.get("clock", 0)
        atualizar_clock(clock_recebido)

        print("\nCanal:", canal)
        print("Mensagem:", payload["mensagem"])
        print("Clock recebido:", clock_recebido)
        print("Clock bot:", clock)
        print("Enviado em:", payload["timestamp_envio"])
        print("Recebido em:", time.time())

    except zmq.Again:
        #  não trava quando não tem mensagem
        time.sleep(0.1)

    except Exception as e:
        print("Erro no bot:", e)