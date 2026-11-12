package chat.java;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ProxyPubSub {
    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket xsub = context.createSocket(SocketType.XSUB);
            xsub.bind("tcp://*:5557"); // Recebe as publicações dos servidores

            ZMQ.Socket xpub = context.createSocket(SocketType.XPUB);
            xpub.bind("tcp://*:5558"); // Distribui para os bots inscritos

            System.out.println("[PROXY] XSUB na 5557 | XPUB na 5558");
            ZMQ.proxy(xsub, xpub, null);
        }
    }
}