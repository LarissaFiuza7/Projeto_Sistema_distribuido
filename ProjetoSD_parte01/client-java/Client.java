import org.zeromq.ZMQ;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Client {
    private static int clock = 0;

    private static byte[] criarMensagem(String tipo, Map<String, Object> dados) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packMapHeader(4);

        packer.packString("timestamp");
        packer.packDouble(System.currentTimeMillis() / 1000.0);

        clock++;
        packer.packString("clock");
        packer.packInt(clock);

        packer.packString("tipo");
        packer.packString(tipo);

        packer.packString("dados");
        if (dados == null || dados.isEmpty()) {
            packer.packMapHeader(0);
        } else {
            packer.packMapHeader(dados.size());
            for (Map.Entry<String, Object> entry : dados.entrySet()) {
                packer.packString(entry.getKey());
                if (entry.getValue() instanceof String) {
                    packer.packString((String) entry.getValue());
                } else if (entry.getValue() instanceof Integer) {
                    packer.packInt((Integer) entry.getValue());
                }
            }
        }

        packer.close();
        return packer.toByteArray();
    }

    private static Value receberEResponder(ZMQ.Socket socket) throws Exception {
        byte[] respostaBytes = socket.recv();
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(respostaBytes);
        Value respostaValue = unpacker.unpackValue();
        unpacker.close();

        if (respostaValue.isMapValue()) {
            MapValue map = respostaValue.asMapValue();
            Map<Value, Value> javaMap = map.map();
            
            for (Map.Entry<Value, Value> entry : javaMap.entrySet()) {
                if (entry.getKey().isStringValue() && entry.getKey().asStringValue().asString().equals("clock")) {
                    if (entry.getValue().isIntegerValue()) {
                        int clockServidor = entry.getValue().asIntegerValue().asInt();
                        clock = Math.max(clock, clockServidor) + 1;
                    }
                }
            }
        }
        return respostaValue;
    }

    public static void main(String[] args) {
        System.out.println("Cliente Java iniciado...");

        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socketReq = context.socket(ZMQ.REQ);
        socketReq.connect("tcp://broker:5555");

        ZMQ.Socket socketSub = context.socket(ZMQ.SUB);
        socketSub.connect("tcp://proxy_pubsub:5558");

        try {
            Map<String, Object> dadosLogin = new HashMap<>();
            dadosLogin.put("usuario", "Larissa-Java");
            socketReq.send(criarMensagem("login", dadosLogin));
            
            Value respLogin = receberEResponder(socketReq);
            System.out.println("Login efetuado. Resposta: " + respLogin);
            System.out.println("----------------------------------------");

            socketReq.send(criarMensagem("listar canais", null));
            Value respCanais = receberEResponder(socketReq);
            
            List<String> canaisExistentes = new ArrayList<>();
            if (respCanais.isMapValue()) {
                MapValue map = respCanais.asMapValue();
                Map<Value, Value> javaMap = map.map();
                
                for (Map.Entry<Value, Value> entry : javaMap.entrySet()) {
                    if (entry.getKey().isStringValue() && entry.getKey().asStringValue().asString().equals("dados")) {
                        Value dadosValue = entry.getValue();
                        if (dadosValue != null && dadosValue.isArrayValue()) {
                            for (Value v : dadosValue.asArrayValue()) {
                                canaisExistentes.add(v.asStringValue().asString());
                            }
                        }
                    }
                }
            }
            System.out.println("Canais disponíveis no servidor: " + canaisExistentes);

            if (canaisExistentes.size() < 5) {
                String novoCanal = "canal_" + (System.currentTimeMillis() / 1000);
                System.out.println("Menos de 5 canais encontrados. Criando o canal: " + novoCanal);
                
                Map<String, Object> dadosNovoCanal = new HashMap<>();
                dadosNovoCanal.put("nome", novoCanal);
                socketReq.send(criarMensagem("channel", dadosNovoCanal));
                
                receberEResponder(socketReq);
                canaisExistentes.add(novoCanal);
            }

            int limiteInscricao = Math.min(canaisExistentes.size(), 3);
            for (int i = 0; i < limiteInscricao; i++) {
                String canal = canaisExistentes.get(i);
                System.out.println("Se inscrevendo no canal (tópico): " + canal);
                socketSub.subscribe(canal.getBytes(ZMQ.CHARSET));
            }
            System.out.println("----------------------------------------");

            System.out.println("Iniciando loop infinito de envio de mensagens...");
            Random random = new Random();

            while (true) {
                String canalEscolhido = "geral";
                if (!canaisExistentes.isEmpty()) {
                    canalEscolhido = canaisExistentes.get(random.nextInt(canaisExistentes.size()));
                }

                System.out.println("\n>>> Iniciando bloco de 10 mensagens no canal: [" + canalEscolhido + "]");

                for (int i = 0; i < 10; i++) {
                    Map<String, Object> dadosPublicacao = new HashMap<>();
                    dadosPublicacao.put("canal", canalEscolhido);
                    dadosPublicacao.put("mensagem", "Mensagem automatizada " + i + " de Larissa-Java");

                    socketReq.send(criarMensagem("publish", dadosPublicacao));
                    
                    Value resposta = receberEResponder(socketReq);
                    System.out.println("[Clock Local: " + clock + "] Confirmação do Servidor: " + resposta);

                    Thread.sleep(1000);
                }

                Thread.sleep(2000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            socketReq.close();
            socketSub.close();
            context.term();
        }
    }
}