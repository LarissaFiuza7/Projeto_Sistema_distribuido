import org.zeromq.ZMQ;
import org.msgpack.core.*;
import org.msgpack.value.*;

public class Server {

    static int clock = 0;
    static String nomeServidor = "servidor-" + System.currentTimeMillis();

    static int meuRank = 0;
    static String coordenadorAtual = null;

    //  HEARTBEAT POR TEMPO
    static long ultimoHeartbeat = System.currentTimeMillis();
    static final int INTERVALO = 5000; // 5 segundos

    public static void main(String[] args) {

        System.out.println("Servidor Java rodando...");

        ZMQ.Context context = ZMQ.context(1);

        ZMQ.Socket socket = context.socket(ZMQ.REP);
        socket.connect("tcp://broker:5556");

        ZMQ.Socket coord = context.socket(ZMQ.REQ);
        coord.connect("tcp://coordenador:5560");

        registrarServidor(coord);

        try {
            Value lista = pedirServidores(coord);
            System.out.println("Lista recebida: " + lista);

            coordenadorAtual = elegerCoordenador(lista);
            System.out.println("Coordenador atual: " + coordenadorAtual);

        } catch (Exception e) {
            e.printStackTrace();
        }

        while (true) {

            byte[] msg = socket.recv();

            try {
                MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(msg);
                Value v = unpacker.unpackValue();

                int clockRecebido = 0;

                if (v.isMapValue()) {
                    MapValue map = v.asMapValue();

                    for (Value key : map.map().keySet()) {
                        if (key.asStringValue().asString().equals("clock")) {
                            clockRecebido = map.map().get(key).asIntegerValue().asInt();
                        }
                    }
                }

                // CLOCK LÓGICO
                clock = Math.max(clock, clockRecebido) + 1;
                System.out.println("Clock servidor: " + clock);

                clock++;

                // 01 HEARTBEAT POR TEMPO
                if (System.currentTimeMillis() - ultimoHeartbeat > INTERVALO) {
                    verificarCoordenador(coord);
                    ultimoHeartbeat = System.currentTimeMillis();
                }

                // 🔹 RESPOSTA
                MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

                packer.packMapHeader(4);

                packer.packString("timestamp");
                packer.packDouble(System.currentTimeMillis() / 1000.0);

                packer.packString("clock");
                packer.packInt(clock);

                packer.packString("tipo");
                packer.packString("resposta");

                packer.packString("dados");
                packer.packString("OK do servidor Java");

                packer.close();

                socket.send(packer.toByteArray());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void registrarServidor(ZMQ.Socket coord) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

            packer.packMapHeader(2);

            packer.packString("tipo");
            packer.packString("register");

            packer.packString("dados");
            packer.packMapHeader(1);
            packer.packString("nome");
            packer.packString(nomeServidor);

            packer.close();

            coord.send(packer.toByteArray());

            byte[] reply = coord.recv();

            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(reply);
            Value v = unpacker.unpackValue();

            System.out.println("Registrado: " + v);

            MapValue map = v.asMapValue();
            Value dados = map.map().get(ValueFactory.newString("dados"));

            meuRank = dados.asMapValue()
                    .map()
                    .get(ValueFactory.newString("rank"))
                    .asIntegerValue()
                    .asInt();

            System.out.println("Meu rank: " + meuRank);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Value pedirServidores(ZMQ.Socket coord) throws Exception {

        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(1);
        packer.packString("tipo");
        packer.packString("get_servers");

        packer.close();

        coord.send(packer.toByteArray());

        byte[] reply = coord.recv();

        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(reply);
        return unpacker.unpackValue();
    }

    public static String elegerCoordenador(Value resposta) {

        int maiorRank = -1;
        String eleito = null;

        MapValue map = resposta.asMapValue();
        Value dados = map.map().get(ValueFactory.newString("dados"));

        if (dados == null || !dados.isArrayValue()) {
            return null;
        }

        for (Value item : dados.asArrayValue()) {

            MapValue servidor = item.asMapValue();

            String nome = servidor.map()
                    .get(ValueFactory.newString("nome"))
                    .asStringValue()
                    .asString();

            int rank = servidor.map()
                    .get(ValueFactory.newString("rank"))
                    .asIntegerValue()
                    .asInt();

            if (rank > maiorRank) {
                maiorRank = rank;
                eleito = nome;
            }
        }

        return eleito;
    }

    public static void verificarCoordenador(ZMQ.Socket coord) {

        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

            packer.packMapHeader(2);

            packer.packString("tipo");
            packer.packString("heartbeat");

            packer.packString("dados");
            packer.packMapHeader(1);
            packer.packString("nome");
            packer.packString(nomeServidor);

            packer.close();

            coord.send(packer.toByteArray());
            coord.recv();

            Value lista = pedirServidores(coord);
            System.out.println("Lista heartbeat: " + lista);

            String novo = elegerCoordenador(lista);

            if (coordenadorAtual == null || !coordenadorAtual.equals(novo)) {
                System.out.println("Novo coordenador: " + novo);
                coordenadorAtual = novo;
            }

        } catch (Exception e) {
            System.out.println("Falha no coordenador");
        }
    }
}