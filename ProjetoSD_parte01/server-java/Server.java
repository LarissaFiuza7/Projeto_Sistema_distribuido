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

    static int clock = 0;
    static int contador = 0;

    static String nome =
            "servidor-" + System.currentTimeMillis();

    static String coordenador = null;

    static List<String> historico =
            new ArrayList<>();

    public static void main(String[] args) {

        ZMQ.Context context = ZMQ.context(1);

        ZMQ.Socket socket =
                context.socket(ZMQ.REP);

        socket.connect("tcp://broker:5556");

        ZMQ.Socket coord =
                context.socket(ZMQ.REQ);

        coord.connect("tcp://coordenador:5560");

        ZMQ.Socket pub =
                context.socket(ZMQ.PUB);

        pub.connect("tcp://proxy_pubsub:5557");

        ZMQ.Socket sub =
                context.socket(ZMQ.SUB);

        sub.connect("tcp://proxy_pubsub:5558");

        sub.subscribe("replica".getBytes());
        sub.subscribe("servers".getBytes());

        registrar(coord);

        Value lista = pedir(coord);

        coordenador = eleger(lista);

        System.out.println(
                "Coord inicial: " + coordenador
        );

        new Thread(
                () -> heartbeat(coord)
        ).start();

        new Thread(
                () -> ouvir(sub)
        ).start();

        while (true) {

            byte[] msg = socket.recv();

            try {

                MessageUnpacker up =
                        MessagePack.newDefaultUnpacker(msg);

                Value v = up.unpackValue();

                int recvClock = 0;

                if (v.isMapValue()) {

                    MapValue map = v.asMapValue();

                    for (Value k : map.map().keySet()) {

                        String key =
                                k.asStringValue().asString();

                        if (key.equals("clock")) {

                            recvClock =
                                    map.map()
                                            .get(k)
                                            .asIntegerValue()
                                            .asInt();
                        }
                    }
                }

                clock =
                        Math.max(clock, recvClock) + 1;

                contador++;

                String reg =
                        nome + " clock=" + clock;

                historico.add(reg);

                Map<String, Object> replica =
                        new HashMap<>();

                replica.put("server", nome);
                replica.put("clock", clock);
                replica.put("msg", v.toString());

                byte[] packed = pack(replica);

                String hex = bytesToHex(packed);

                pub.send(
                        ("replica " + hex).getBytes()
                );

                if (contador >= 15) {

                    sync(coord);

                    contador = 0;
                }

                MessageBufferPacker p =
                        MessagePack.newDefaultBufferPacker();

                p.packMapHeader(2);

                p.packString("clock");
                p.packInt(clock);

                p.packString("tipo");
                p.packString("resposta");

                p.close();

                socket.send(p.toByteArray());

            } catch (Exception e) {

                e.printStackTrace();
            }
        }
    }

    // =========================
    // ELEIÇÃO
    // =========================

    static String eleger(Value v) {

        MapValue map = v.asMapValue();

        Value dados =
                map.map().get(
                        ValueFactory.newString("dados")
                );

        String eleito = null;

        int maior = -1;

        for (Value s : dados.asArrayValue()) {

            MapValue m = s.asMapValue();

            String nome =
                    m.map()
                            .get(
                                    ValueFactory.newString("nome")
                            )
                            .asStringValue()
                            .asString();

            int rank =
                    m.map()
                            .get(
                                    ValueFactory.newString("rank")
                            )
                            .asIntegerValue()
                            .asInt();

            if (rank > maior) {

                maior = rank;

                eleito = nome;
            }
        }

        return eleito;
    }

    // =========================
    // HEARTBEAT
    // =========================

    static void heartbeat(ZMQ.Socket coord) {

        while (true) {

            try {

                MessageBufferPacker p =
                        MessagePack.newDefaultBufferPacker();

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

                Thread.sleep(2000);

            } catch (Exception e) {

                System.out.println("heartbeat erro");
            }
        }
    }

    // =========================
    // BERKELEY
    // =========================

    static void sync(ZMQ.Socket coord) {

        try {

            coord.send(
                    packSimple("sync_clock")
            );

            coord.recv();

        } catch (Exception e) {

            System.out.println("erro sync");
        }
    }

    // =========================
    // PACK SIMPLE
    // =========================

    static byte[] packSimple(String t) {

        try {

            MessageBufferPacker p =
                    MessagePack.newDefaultBufferPacker();

            p.packMapHeader(1);

            p.packString("tipo");
            p.packString(t);

            p.close();

            return p.toByteArray();

        } catch (Exception e) {

            return new byte[0];
        }
    }

    // =========================
    // PACK
    // =========================

    static byte[] pack(Map<String, Object> m) {

        try {

            MessageBufferPacker p =
                    MessagePack.newDefaultBufferPacker();

            p.packMapHeader(m.size());

            for (String k : m.keySet()) {

                p.packString(k);

                Object v = m.get(k);

                if (v instanceof String) {

                    p.packString((String) v);

                } else if (v instanceof Integer) {

                    p.packInt((Integer) v);

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

    // =========================
    // HEX
    // =========================

    static String bytesToHex(byte[] bytes) {

        StringBuilder sb = new StringBuilder();

        for (byte b : bytes) {

            sb.append(
                    String.format("%02x", b)
            );
        }

        return sb.toString();
    }

    // =========================
    // REGISTRAR
    // =========================

    static void registrar(ZMQ.Socket coord) {

        try {

            MessageBufferPacker p =
                    MessagePack.newDefaultBufferPacker();

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

            System.out.println(
                    "Servidor Java registrado"
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    // =========================
    // PEDIR SERVIDORES
    // =========================

    static Value pedir(ZMQ.Socket coord) {

        try {

            MessageBufferPacker p =
                    MessagePack.newDefaultBufferPacker();

            p.packMapHeader(1);

            p.packString("tipo");
            p.packString("get_servers");

            p.close();

            coord.send(p.toByteArray());

            byte[] resp = coord.recv();

            MessageUnpacker up =
                    MessagePack.newDefaultUnpacker(resp);

            return up.unpackValue();

        } catch (Exception e) {

            e.printStackTrace();

            return null;
        }
    }

    // =========================
    // OUVIR PUBSUB
    // =========================

    static void ouvir(ZMQ.Socket sub) {

        while (true) {

            try {

                String msg =
                        new String(sub.recv());

                if (msg.contains("replica")) {

                    System.out.println(
                            "Replica: " + msg
                    );
                }

            } catch (Exception e) {

                e.printStackTrace();
            }
        }
    }
}