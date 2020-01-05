package edu.yu.cs.fall2019.intro_to_distributed;

import java.net.InetSocketAddress;
import java.util.HashMap;


public class Driver {
    private static int[] ports = {8000, 8010, 8020, 8030, 8040, 8050, 8060, 8070};
    private static int GATEWAYPORT = Config.GTWYINTRNL;//Hardcoded
    public static void main(String[] args) {
        if(args.length != 1) {
            return;
        }

        int myID = Integer.valueOf(args[0]);

        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < ports.length; i++)
        {
            peerIDtoAddress.put((long)i, new InetSocketAddress("localhost", ports[i]));
        }

        peerIDtoAddress.remove((long)myID);
        ZooKeeperPeerServer server;
        if(ports[myID] == GATEWAYPORT) {
            server = new Gateway(ports[myID], 0, myID, peerIDtoAddress);

        } else {
            server = new ZooKeeperPeerServerImpl(ports[myID], 0, myID, peerIDtoAddress);
        }
        new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
    }
}
