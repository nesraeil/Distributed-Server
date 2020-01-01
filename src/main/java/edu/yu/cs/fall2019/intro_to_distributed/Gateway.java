package edu.yu.cs.fall2019.intro_to_distributed;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static edu.yu.cs.fall2019.intro_to_distributed.Util.startAsDaemon;

public class Gateway implements ZooKeeperPeerServer {

    private final int GATEWAYPORT = 9999;

    private final int myPort;
    private long peerEpoch;
    private Long id;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;
    private final InetSocketAddress myAddress;
    private ServerState state;
    private volatile boolean shutdown;
    private volatile Vote currentLeader;
    private long requestID;

    //Networking stuff
    private LinkedBlockingQueue<Message> outgoingUDP;
    private LinkedBlockingQueue<Message> incomingUDP;
    private UDPMessageSender senderWorkerUDP;
    private UDPMessageReceiver receiverWorkerUDP;
    private LinkedBlockingQueue<Message> outgoingTCP;
    private LinkedBlockingQueue<Message> incomingTCP;
    private TCPMessageSender senderWorkerTCP;
    private TCPMessageReceiver receiverWorkerTCP;
    private LinkedBlockingQueue<Message> incomingHeartGossip;
    private HttpServer httpServer;

    //Heartbeat runner and list of dead servers that it updates
    private Heartbeat heart;

    //Client work trackers
    private LinkedBlockingQueue<ClientRequest> workFromClientBuff;
    private HashMap<Long, ClientRequest> requestIdToWork;




    public Gateway(int myPort, long peerEpoch, long id, HashMap<Long, InetSocketAddress> peerIDtoAddress) {
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = new ConcurrentHashMap<>(peerIDtoAddress);
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.state = ServerState.OBSERVING;
        this.shutdown = false;
        this.requestID = 0;

        //Hearbeat and gossip stuff
        this.incomingHeartGossip = new LinkedBlockingQueue<>();
        heart = new Heartbeat(this, incomingHeartGossip, this.peerIDtoAddress);

        //UDP Stuff
        this.outgoingUDP = new LinkedBlockingQueue<>();
        this.incomingUDP = new LinkedBlockingQueue<>();
        this.senderWorkerUDP = new UDPMessageSender(this.outgoingUDP);
        this.receiverWorkerUDP = new UDPMessageReceiver(this.incomingUDP,this.incomingHeartGossip,this.peerIDtoAddress, this.myAddress,this.myPort);

        //TCP Stuff
        this.outgoingTCP = new LinkedBlockingQueue<>();
        this.incomingTCP = new LinkedBlockingQueue<>();
        this.senderWorkerTCP = new TCPMessageSender(this.outgoingTCP);
        this.receiverWorkerTCP = new TCPMessageReceiver(this.incomingTCP, this.peerIDtoAddress, this.myPort);

        //Work from client queue
        workFromClientBuff = new LinkedBlockingQueue<>();

        //Hardcoded http public port

        try {
            httpServer = HttpServer.create(new InetSocketAddress(GATEWAYPORT), 0);
            httpServer.createContext("/compileandrun", new MyHandler());
            httpServer.setExecutor(null);
            System.out.println("starting http server on port: " + GATEWAYPORT);
        } catch (IOException e) {
            throw new RuntimeException("Could not start gateway server at port " + GATEWAYPORT);
        }

        //Misc
        requestID = 0;
        this.workFromClientBuff = new LinkedBlockingQueue<>();
        this.requestIdToWork = new HashMap<>();

    }


    @Override
    public void shutdown() {
        shutdown = true;
        senderWorkerUDP.shutdown();
        receiverWorkerUDP.shutdown();
        senderWorkerTCP.shutdown();
        receiverWorkerTCP.shutdown();
        heart.shutdown();
        httpServer.stop(0);
    }

    @Override
    public void run() {

        startAsDaemon(senderWorkerUDP, "UDP sender thread for " + this.myAddress.getPort());
        startAsDaemon(receiverWorkerUDP, "UDP receiving thread for " + this.myAddress.getPort());
        startAsDaemon(senderWorkerTCP, "TCP sender thread for " + this.myAddress.getPort());
        startAsDaemon(receiverWorkerTCP, "TCP receiving thread for " + this.myAddress.getPort());
        httpServer.start();
        startAsDaemon(heart, "heartbeat thread for " + this.myAddress.getPort());

        while (!shutdown) {

            if(currentLeader == null || !peerIDtoAddress.containsKey(currentLeader.getCandidateID())) {
                setCurrentLeader(lookForLeader());
                System.out.println("Gateway is ready to accept messages");
            }
            //Try to send out next thing in incoming work queue
            ClientRequest work = null;
            try {
                work = workFromClientBuff.poll(1, TimeUnit.SECONDS);
                if(work != null) {
                    requestID++;
                    sendMessage(Message.MessageType.WORK, work.requestBody, peerIDtoAddress.get(currentLeader.getCandidateID()));
                    requestIdToWork.put(requestID,work);

                }
            } catch (InterruptedException e) {
                continue;
            }

            if(incomingTCP.peek() != null) {
                Message message = incomingTCP.poll();
                switch (message.getMessageType()) {
                    case COMPLETED_WORK:
                        returnCompletedWork(message.getRequestID(), message.getMessageContents());
                        break;
                    default:
                        System.err.println("Gateway received non completed_work message from leader");
                        break;
                }
            }
        }
    }

    private void returnCompletedWork(long reqId, byte[] result) {
        ClientRequest request = requestIdToWork.get(reqId);
        HttpExchange client = request.client;
        OutputStream os;
        try {
            client.sendResponseHeaders(getResultHeader(result), result.length);
            os = client.getResponseBody();
            os.write(result);
            os.close();
        } catch (IOException e) {
            System.err.println("Problem sending response to client");
            e.printStackTrace();
        }
        requestIdToWork.remove(reqId);
    }

    private int getResultHeader(byte[] result) {
        String resStr = new String(result);
        if(resStr.startsWith("400")) {
            return 400;
        }
        return 200;
    }

    /*
    private void acceptWorkFromClients(){
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!shutdown) {
                    //Takes http requests and puts contents into buffer to be read by client
                    //Will hold work in that buffer if there is no current leader
                    try {
                        Socket client = httpServer.accept();
                        InputStream is = client.getInputStream();
                        byte[] work = Util.readAllBytes(is);
                        ClientRequest cr = new ClientRequest(client, work);
                        workFromClientBuff.offer(cr);

                    } catch (IOException e) {
                        System.err.println("Error accepting work from client");
                        e.printStackTrace();
                    }
                }
            }
        });
        t1.start();
    }*/

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
        if (type == Message.MessageType.ELECTION || type == Message.MessageType.HEARTBEAT || type == Message.MessageType.GOSSIP) {
            Message m = new Message(type, messageContents, myAddress.getHostName(), myAddress.getPort(), target.getHostName(), target.getPort());
            outgoingUDP.offer(m);
        } else {
            Message m = new Message(type, messageContents, myAddress.getHostName(), myAddress.getPort(), target.getHostName(), target.getPort(), requestID);
            outgoingTCP.offer(m);
        }

    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress address: peerIDtoAddress.values()) {
            sendMessage(type, messageContents, address);
        }
    }

    @Override
    public ServerState getPeerState() {
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
        return null;
    }

    @Override
    public int getQuorumSize() {
        //Subtract one for the Gateway
        return peerIDtoAddress.size() - 1;
    }

    private Vote lookForLeader()
    {
        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this,this.incomingUDP);
        return election.lookForLeader();
    }

    class ClientRequest {
        HttpExchange client;
        byte[] requestBody;

        ClientRequest(HttpExchange client, byte[] requestBody) {
            this.client = client;
            this.requestBody = requestBody;
        }
    }

    class MyHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            InputStream is = t.getRequestBody();
            byte[] work = Util.readAllBytes(is);
            ClientRequest cr = new ClientRequest(t, work);
            workFromClientBuff.offer(cr);
        }
    }
}
