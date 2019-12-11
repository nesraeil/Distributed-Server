package edu.yu.cs.fall2019.intro_to_distributed;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static edu.yu.cs.fall2019.intro_to_distributed.Util.startAsDaemon;

public class ZooKeeperPeerServerImpl implements ZooKeeperPeerServer
{
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private HashMap<Long,InetSocketAddress> peerIDtoAddress;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private int requestID;
    private JavaRunnerFollower follower;
    private RoundRobinLeader leader;

    ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, HashMap<Long,InetSocketAddress> peerIDtoAddress)
    {
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
        this.myAddress = new InetSocketAddress("localhost", myPort);//Check this
        this.outgoingMessages = new LinkedBlockingQueue<>();//Check this
        this.incomingMessages = new LinkedBlockingQueue<>();//Check this
        this.senderWorker = new UDPMessageSender(this.outgoingMessages);//Check this
        this.receiverWorker = new UDPMessageReceiver(this.incomingMessages,this.myAddress,this.myPort);//Check this
        this.state = ServerState.LOOKING;//Should i set my state here
        requestID = 0;
    }

    @Override
    public void shutdown()
    {
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
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
        //step 1: create and start thread that sends broadcast messages
        startAsDaemon(senderWorker, "sender thread for " + this.myAddress.getPort());
        //step 2: create and start thread that listens for messages sent to this server
        startAsDaemon(receiverWorker, "receiving thread for " + this.myAddress.getPort());
        //step 3: main server loop
        try
        {
            while (!this.shutdown)
            {

                switch (getPeerState())
                {
                    case OBSERVING:
                    case LOOKING:
                        //start leader election
                        setCurrentLeader(lookForLeader());
                        break;
                    case FOLLOWING:
                        follower = new JavaRunnerFollower(this, incomingMessages, outgoingMessages);
                        follower.start();
                        break;
                    case LEADING:
                        leader = new RoundRobinLeader(this, incomingMessages, outgoingMessages, peerIDtoAddress);
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
        requestID++;
        Message m = new Message(type, messageContents, myAddress.getHostName(), myAddress.getPort(), target.getHostName(), target.getPort(), requestID);
        outgoingMessages.offer(m);
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
        return peerIDtoAddress.size();
    }

    private Vote lookForLeader()
    {

        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this,this.incomingMessages);
        return election.lookForLeader();
    }

}