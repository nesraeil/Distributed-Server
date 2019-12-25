package edu.yu.cs.fall2019.intro_to_distributed;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static edu.yu.cs.fall2019.intro_to_distributed.Util.startAsDaemon;

public class ZooKeeperPeerServerImpl implements ZooKeeperPeerServer
{
    //Server info
    private final int myPort;
    private long peerEpoch;
    private long id;
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

    //Runners for being a follower/leader
    private JavaRunnerFollower follower;
    private RoundRobinLeader leader;

    //Heartbeat runner and list of dead servers that it updates
    private Heartbeat heart;


    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, HashMap<Long, InetSocketAddress> peerIDtoAddress)
    {
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = new ConcurrentHashMap<>(peerIDtoAddress);
        this.myAddress = new InetSocketAddress("localhost", myPort);//Check this

        //Hearbeat and gossip incoming queues
        this.incomingHeartGossip = new LinkedBlockingQueue<>();

        //UDP Stuff
        this.outgoingUDP = new LinkedBlockingQueue<>();
        this.incomingUDP = new LinkedBlockingQueue<>();
        this.senderWorkerUDP = new UDPMessageSender(this.outgoingUDP);
        this.receiverWorkerUDP = new UDPMessageReceiver(this.incomingUDP,this.incomingHeartGossip, this.peerIDtoAddress, this.myAddress,this.myPort);

        //TCP Stuff
        this.outgoingTCP = new LinkedBlockingQueue<>();
        this.incomingTCP = new LinkedBlockingQueue<>();
        this.senderWorkerTCP = new TCPMessageSender(this.outgoingTCP);
        this.receiverWorkerTCP = new TCPMessageReceiver(this.incomingTCP, this.peerIDtoAddress, this.myPort);

        this.state = ServerState.LOOKING;
        this.requestID = 0;

        heart = new Heartbeat(this, incomingHeartGossip, this.peerIDtoAddress);
    }

    @Override
    public void shutdown()
    {
        shutdown = true;
        senderWorkerUDP.shutdown();
        receiverWorkerUDP.shutdown();
        senderWorkerTCP.shutdown();
        receiverWorkerTCP.shutdown();
        heart.shutdown();
        if(follower != null) {
            follower.shutdown();
        }
        if(leader != null) {
            leader.shutdown();
        }

    }

    @Override
    public void run()
    {
        startAsDaemon(senderWorkerUDP, "UDP sender thread for " + this.myAddress.getPort());
        startAsDaemon(receiverWorkerUDP, "UDP receiving thread for " + this.myAddress.getPort());
        startAsDaemon(senderWorkerTCP, "TCP sender thread for " + this.myAddress.getPort());
        startAsDaemon(receiverWorkerTCP, "TCP receiving thread for " + this.myAddress.getPort());
        //startAsDaemon(heart, "Heartbeat thread for " + this.myAddress.getPort());

        //step 3: main server loop
        try
        {
            while (!this.shutdown)
            {
                switch (getPeerState())
                {
                    case LOOKING:
                        //start leader election
                        peerEpoch++;
                        setCurrentLeader(lookForLeader());
                        peerEpoch = currentLeader.getPeerEpoch();
                        break;
                    case FOLLOWING:
                        follower = new JavaRunnerFollower(this, incomingTCP, outgoingTCP, incomingUDP, outgoingUDP);
                        follower.start();
                        break;
                    case LEADING:
                        leader = new RoundRobinLeader(this, incomingTCP, outgoingTCP, incomingUDP, outgoingUDP, peerIDtoAddress);
                        leader.start();
                        break;

                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
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

    //Check this
    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        //requestID++;
        Message m = new Message(type, messageContents, myAddress.getHostName(), myAddress.getPort(), target.getHostName(), target.getPort());

        if (type == Message.MessageType.ELECTION || type == Message.MessageType.HEARTBEAT || type == Message.MessageType.GOSSIP) {
            outgoingUDP.offer(m);
        } else {
            outgoingTCP.offer(m);
        }
    }
    //Check this
    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress address: peerIDtoAddress.values()) {
            sendMessage(type, messageContents, address);
        }
    }

    //Check this
    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    //Check this
    @Override
    public void setPeerState(ServerState newState) {
        this.state = newState;
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

    //Check this
    @Override
    public InetSocketAddress getPeerByID(long id) {

        return peerIDtoAddress.get(id);
    }

    //Check this
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

}