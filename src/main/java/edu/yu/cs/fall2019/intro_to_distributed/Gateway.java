package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static edu.yu.cs.fall2019.intro_to_distributed.Util.startAsDaemon;

public class Gateway implements ZooKeeperPeerServer {

    private final int myPort;
    private volatile boolean shutdown;
    private HashMap<Integer, Message> recievedWork;
    private final ServerState state;
    private ServerSocket httpServer;

    private final InetSocketAddress myAddress;

    private LinkedBlockingQueue<Message> outgoingUDP;
    private LinkedBlockingQueue<Message> incomingUDP;
    private UDPMessageSender senderWorkerUDP;
    private UDPMessageReceiver receiverWorkerUDP;
    private LinkedBlockingQueue<Message> outgoingTCP;
    private LinkedBlockingQueue<Message> incomingTCP;
    private TCPMessageSender senderWorkerTCP;
    private TCPMessageReceiver receiverWorkerTCP;

    private Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private HashMap<Long,InetSocketAddress> peerIDtoAddress;
    private int requestID;

    public Gateway(int myPort, Long id, HashMap<Long,InetSocketAddress> peerIDtoAddress){
        this.myPort = myPort;
        shutdown = false;
        recievedWork = new HashMap<>();
        state = ServerState.OBSERVING;
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.id = id;

        this.outgoingUDP = new LinkedBlockingQueue<>();
        this.incomingUDP = new LinkedBlockingQueue<>();
        this.senderWorkerUDP = new UDPMessageSender(this.outgoingUDP);
        this.receiverWorkerUDP = new UDPMessageReceiver(incomingUDP, );

        //TCP Stuff
        this.outgoingTCP = new LinkedBlockingQueue<>();
        this.incomingTCP = new LinkedBlockingQueue<>();
        this.senderWorkerTCP = new TCPMessageSender(this.outgoingTCP);
        this.receiverWorkerTCP = new TCPMessageReceiver(this.incomingTCP, this.myPort);

        try {
            httpServer = new ServerSocket(myPort);
        } catch (IOException e) {
            throw new RuntimeException("Could not start server at port " + myPort);
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public void run() {
        //step 1: create and start thread that sends UDP messages
        startAsDaemon(senderWorkerTCP, "sender thread for Gateway at port" + myPort);
        //step 2: create and start thread that listens for UDP messages sent to this server
        startAsDaemon(receiverWorkerTCP, "receiving thread for " + this.myAddress.getPort());

        while (!shutdown) {
            Socket client = null;
            try {
                client = httpServer.accept();
            } catch (IOException e) {
                throw new RuntimeException("Error accepting connection");
            }

        }

    }

    @Override
    public void setCurrentLeader(Vote v) {
        currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {

    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {

    }

    @Override
    public ServerState getPeerState() {
        //Will always be observing
        return state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        if(newState != ServerState.OBSERVING) {
            System.err.println("Gateway peer state can only be OBSERVING");
        }
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public long getPeerEpoch() {
        return peerEpoch;
    }

    @Override
    public InetSocketAddress getMyAddress() {
        return myAddress;
    }

    @Override
    public int getMyPort() {
        return myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long id) {
        return;
    }

    @Override
    public int getQuorumSize() {
        return 0;
    }
}
