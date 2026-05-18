import org.zeromq.ZMQ;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Server {

    static int clockLogico = 0;
    static long relogioFisicoLocal = System.currentTimeMillis() / 1000;
    static int contador = 0;
    static String nome = "servidor-java-" + System.currentTimeMillis();
    static String coordenador = null;
    static List<String> historico = new ArrayList<>();
    static Map<String, Long> temposRecebidos = new HashMap<>();

    public static void main(String[] args) {

        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.REP);
        socket.connect("tcp://broker:5556");

        ZMQ.Socket coord = context.socket(ZMQ.REQ);
        coord.connect("tcp://coordenador:5560");

        ZMQ.Socket pub = context.socket(ZMQ.PUB);
        pub.connect("tcp://proxy_pubsub:5557");

        ZMQ.Socket sub = context.socket(ZMQ.SUB);
        sub.connect("tcp://proxy_pubsub:5558");

        sub.subscribe("replica".getBytes());
        sub.subscribe("servers".getBytes());
        sub.subscribe("berkeley".getBytes());

        registrar(coord);
        Value lista = pedir(coord);
        coordenador = eleger(lista);

        System.out.println("Coord inicial: " + coordenador);

        // Thread para simular o passar do tempo físico do relógio
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    relogioFisicoLocal++;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        // Thread para ouvir canais Pub-Sub (Replicação, Coordenação e Berkeley)
        new Thread(() -> ouvir(sub, pub)).start();

        while (true) {
            byte[] msg = socket.recv();

            try {
                MessageUnpacker up = MessagePack.newDefaultUnpacker(msg);
                Value v = up.unpackValue();
                int recvClock = 0;

                if (v.isMapValue()) {
                    MapValue map = v.asMapValue();
                    for (Value k : map.map().keySet()) {
                        String key = k.asStringValue().asString();
                        if (key.equals("clock")) {
                            recvClock = map.map().get(k).asIntegerValue().asInt();
                        }
                    }
                }

                clockLogico = Math.max(clockLogico, recvClock) + 1;
                contador++;

                String reg = nome + " clock=" + clockLogico;
                historico.add(reg);

                Map<String, Object> replica = new HashMap<>();
                replica.put("server", nome);
                replica.put("clock", clockLogico);
                replica.put("msg", v.toString());

                byte[] packed = pack(replica);
                String hex = bytesToHex(packed);
                pub.send(("replica " + hex).getBytes());

                if (contador >= 10) {
                    enviarHeartbeatSincrono(coord);
                    
                    Value listaAtualizada = pedir(coord);
                    String novo = eleger(listaAtualizada);
                    if (novo != null && !novo.equals(coordenador)) {
                        coordenador = novo;
                        publicarCoord(pub, novo);
                    }
                    
                    if (contador >= 15) {
                        executarBerkeley(pub);
                        contador = 0;
                    }
                }

                MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
                p.packMapHeader(2);
                p.packString("clock");
                p.packInt(clockLogico);
                p.packString("tipo");
                p.packString("resposta");
                p.close();

                socket.send(p.toByteArray());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void executarBerkeley(ZMQ.Socket pub) {
        if (coordenador == null || !coordenador.equals(nome)) {
            return;
        }

        try {
            System.out.println("[Berkeley] Iniciando checagem de relógios como Coordenador...");
            temposRecebidos.clear();
            temposRecebidos.put(nome, relogioFisicoLocal);

            Map<String, Object> reqMsg = new HashMap<>();
            reqMsg.put("tipo", "req_tempo");
            reqMsg.put("remetente", nome);

            byte[] packed = pack(reqMsg);
            pub.send(("berkeley " + bytesToHex(packed)).getBytes());

            Thread.sleep(1500);

            if (temposRecebidos.size() <= 1) return;

            long soma = 0;
            for (long t : temposRecebidos.values()) {
                soma += t;
            }
            long media = soma / temposRecebidos.size();

            Map<String, Object> ajusteMsg = new HashMap<>();
            ajusteMsg.put("tipo", "ajuste_tempo");

            Map<String, Integer> ajustes = new HashMap<>();
            for (Map.Entry<String, Long> entry : temposRecebidos.entrySet()) {
                ajustes.put(entry.getKey(), (int) (media - entry.getValue()));
            }
            ajusteMsg.put("ajustes", ajustes);

            byte[] packedAjuste = pack(ajusteMsg);
            pub.send(("berkeley " + bytesToHex(packedAjuste)).getBytes());

        } catch (Exception e) {
            System.out.println("Erro ao rodar Berkeley");
        }
    }

    static void ouvir(ZMQ.Socket sub, ZMQ.Socket pub) {
        while (true) {
            try {
                String msgCompleta = new String(sub.recv(), ZMQ.CHARSET);
                String[] partes = msgCompleta.split(" ", 2);
                if (partes.length < 2) continue;

                String canal = partes[0];
                String payloadHex = partes[1];

                byte[] payloadBytes = hexToBytes(payloadHex);
                MessageUnpacker up = MessagePack.newDefaultUnpacker(payloadBytes);
                Value payload = up.unpackValue();
                up.close();

                if (canal.equals("servers")) {
                    if (payload.isMapValue()) {
                        String novo = payload.asMapValue().map().get(ValueFactory.newString("coordenador")).asStringValue().asString();
                        if (!novo.equals(coordenador)) {
                            coordenador = novo;
                            System.out.println("Novo coordenador definido: " + coordenador);
                        }
                    }
                } else if (canal.equals("replica")) {
                    System.out.println("Replica recebida em Java: " + payload);
                } else if (canal.equals("berkeley")) {
                    if (payload.isMapValue()) {
                        MapValue map = payload.asMapValue();
                        String tipoB = map.map().get(ValueFactory.newString("tipo")).asStringValue().asString();

                        if (tipoB.equals("req_tempo") && !nome.equals(coordenador)) {
                            Map<String, Object> respTempo = new HashMap<>();
                            respTempo.put("tipo", "resp_tempo");
                            respTempo.put("remetente", nome);
                            respTempo.put("tempo", relogioFisicoLocal);

                            byte[] packed = pack(respTempo);
                            pub.send(("berkeley " + bytesToHex(packed)).getBytes());
                        } else if (tipoB.equals("resp_tempo") && nome.equals(coordenador)) {
                            String sender = map.map().get(ValueFactory.newString("remetente")).asStringValue().asString();
                            long tempoSrv = map.map().get(ValueFactory.newString("tempo")).asIntegerValue().asLong();
                            temposRecebidos.put(sender, tempoSrv);
                        } else if (tipoB.equals("ajuste_tempo")) {
                            Value ajustesVal = map.map().get(ValueFactory.newString("ajustes"));
                            if (ajustesVal != null && ajustesVal.isMapValue()) {
                                MapValue mapAjustes = ajustesVal.asMapValue();
                                Value meuAjuste = mapAjustes.map().get(ValueFactory.newString(nome));
                                if (meuAjuste != null && meuAjuste.isIntegerValue()) {
                                    int adj = meuAjuste.asIntegerValue().asInt();
                                    relogioFisicoLocal += adj;
                                    System.out.println("[Berkeley] Relógio ajustado em " + adj + "s. Novo tempo: " + relogioFisicoLocal);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static String eleger(Value v) {
        if (v == null) return null;
        try {
            MapValue map = v.asMapValue();
            Value dados = map.map().get(ValueFactory.newString("dados"));
            String eleito = null;
            int maior = -1;

            for (Value s : dados.asArrayValue()) {
                MapValue m = s.asMapValue();
                String nomeSrv = m.map().get(ValueFactory.newString("nome")).asStringValue().asString();
                int rankSrv = m.map().get(ValueFactory.newString("rank")).asIntegerValue().asInt();

                if (rankSrv > maior) {
                    maior = rankSrv;
                    eleito = nomeSrv;
                }
            }
            return eleito;
        } catch (Exception e) {
            return null;
        }
    }

    static void enviarHeartbeatSincrono(ZMQ.Socket coord) {
        try {
            MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
            p.packMapHeader(2);
            p.packString("tipo");
            p.packString("heartbeat");
            p.packString("dados");
            p.packMapHeader(1);
            p.packString("nome");
            p.packString(nome);
            p.close();

            coord.send(p.toByteArray());
            coord.recv();
        } catch (Exception e) {
            System.out.println("Erro ao enviar heartbeat");
        }
    }

    static void publicarCoord(ZMQ.Socket pub, String nomeCoord) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("tipo", "coordenador");
            payload.put("coordenador", nomeCoord);
            byte[] packed = pack(payload);
            String hex = bytesToHex(packed);
            pub.send(("servers " + hex).getBytes());
        } catch (Exception e) {
            System.out.println("Erro ao publicar coordenador");
        }
    }

    static byte[] pack(Map<String, Object> m) {
        try {
            MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
            p.packMapHeader(m.size());
            for (String k : m.keySet()) {
                p.packString(k);
                Object v = m.get(k);
                if (v instanceof String) {
                    p.packString((String) v);
                } else if (v instanceof Integer) {
                    p.packInt((Integer) v);
                } else if (v instanceof Long) {
                    p.packLong((Long) v);
                } else if (v instanceof Map) {
                    Map<?, ?> subMap = (Map<?, ?>) v;
                    p.packMapHeader(subMap.size());
                    for (Map.Entry<?, ?> entry : subMap.entrySet()) {
                        p.packString(entry.getKey().toString());
                        if (entry.getValue() instanceof Integer) {
                            p.packInt((Integer) entry.getValue());
                        } else if (entry.getValue() instanceof Long) {
                            p.packLong((Long) entry.getValue());
                        } else {
                            p.packString(entry.getValue().toString());
                        }
                    }
                } else {
                    p.packString(v.toString());
                }
            }
            p.close();
            return p.toByteArray();
        } catch (Exception e) {
            return new byte[0];
        }
    }

    static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i+1), 16));
        }
        return data;
    }

    static void registrar(ZMQ.Socket coord) {
        try {
            MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
            p.packMapHeader(2);
            p.packString("tipo");
            p.packString("register");
            p.packString("dados");
            p.packMapHeader(1);
            p.packString("nome");
            p.packString(nome);
            p.close();

            coord.send(p.toByteArray());
            coord.recv();
            System.out.println("Servidor Java registrado");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static Value pedir(ZMQ.Socket coord) {
        try {
            MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
            p.packMapHeader(1);
            p.packString("tipo");
            p.packString("get_servers");
            p.close();

            coord.send(p.toByteArray());
            byte[] resp = coord.recv();
            MessageUnpacker up = MessagePack.newDefaultUnpacker(resp);
            return up.unpackValue();
        } catch (Exception e) {
            return null;
        }
    }
}