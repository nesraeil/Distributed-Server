package edu.yu.cs.fall2019.intro_to_distributed;

import edu.yu.cs.fall2019.intro_to_distributed.Message;
import edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;


class RoundRobinLeader {
    private ZooKeeperPeerServer leader;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private HashMap<Long, InetSocketAddress> peerIDtoAddress;
    private long requestID;
    private Iterator<InetSocketAddress> servers;
    private boolean shutdown;

    RoundRobinLeader(ZooKeeperPeerServer leader ,LinkedBlockingQueue<Message> incomingMessages,
                     LinkedBlockingQueue<Message> outgoingMessages,HashMap<Long, InetSocketAddress> peerIDtoAddress) {
        this.leader = leader;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.peerIDtoAddress = peerIDtoAddress;
        requestID = 0;
        servers = peerIDtoAddress.values().iterator();
        shutdown = false;
    }

    void start() {
        HashMap<Long, InetSocketAddress> requestToClientAddress = new HashMap<>();

        while(!shutdown) {
            if(incomingMessages.peek() != null) {
                Message message = incomingMessages.poll();
                //System.out.println(leader.getMyPort() + " got " + message.getMessageType() + " message from " + message.getSenderPort());
                switch (message.getMessageType()) {
                    case WORK:
                        //Sending work to peer servers on round robin basis
                        if(!servers.hasNext()) {
                            servers = peerIDtoAddress.values().iterator();
                        }
                        sendWork(message, servers.next());
                        requestToClientAddress.put(requestID, new InetSocketAddress(message.getSenderHost(), message.getSenderPort()));
                        break;
                    case COMPLETED_WORK:
                        leader.sendMessage(message.getMessageType(), message.getMessageContents(), requestToClientAddress.get(message.getRequestID()));
                        requestToClientAddress.remove(message.getRequestID());
                        break;
                }

            } else {
                try {
                    Thread.sleep(10);
                } catch (Exception e){}
            }
        }
    }

    private void sendWork(Message message, InetSocketAddress worker) {
        requestID++;
        Message work = new Message(message.getMessageType(),
                message.getMessageContents(),
                leader.getMyAddress().getHostName(),
                leader.getMyAddress().getPort(),
                worker.getHostName(),
                worker.getPort(),requestID);
        outgoingMessages.offer(work);
    }

    void shutdown() {
        shutdown = true;

    }
}