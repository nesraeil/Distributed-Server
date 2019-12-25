package edu.yu.cs.fall2019.intro_to_distributed;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;


class RoundRobinLeader {
    //Hardcoded gateway address
    private InetSocketAddress GATEWAYADDRESS = new InetSocketAddress("localhost", 8090);

    private ZooKeeperPeerServer leader;
    private LinkedBlockingQueue<Message> incomingMessagesTCP;
    private LinkedBlockingQueue<Message> outgoingMessagesTCP;
    private LinkedBlockingQueue<Message> incomingMessagesUDP;
    private LinkedBlockingQueue<Message> outgoingMessagesUDP;
    private HashMap<Long, InetSocketAddress> peerIDtoAddress;
    private long requestID;
    private Iterator<InetSocketAddress> servers;
    private boolean shutdown;

    RoundRobinLeader(ZooKeeperPeerServer leader ,
                     LinkedBlockingQueue<Message> incomingMessagesTCP,
                     LinkedBlockingQueue<Message> outgoingMessagesTCP,
                     LinkedBlockingQueue<Message> incomingMessagesUDP,
                     LinkedBlockingQueue<Message> outgoingMessagesUDP,
                     HashMap<Long, InetSocketAddress> peerIDtoAddress) {
        this.leader = leader;
        this.incomingMessagesTCP = incomingMessagesTCP;
        this.outgoingMessagesTCP = outgoingMessagesTCP;
        this.incomingMessagesUDP = incomingMessagesUDP;
        this.outgoingMessagesUDP = outgoingMessagesUDP;

        this.peerIDtoAddress = peerIDtoAddress;
        requestID = 0;
        servers = peerIDtoAddress.values().iterator();
        shutdown = false;
    }

    void start() {
        //HashMap<Long, InetSocketAddress> requestToClientAddress = new HashMap<>();

        while(!shutdown) {
            if(incomingMessagesTCP.peek() != null) {
                Message message = incomingMessagesTCP.poll();
                //System.out.println(leader.getMyPort() + " got " + message.getMessageType() + " message from " + message.getSenderPort());
                switch (message.getMessageType()) {
                    case WORK:
                        //Sending work to peer servers on round robin basis
                        if(!servers.hasNext()) {
                            servers = peerIDtoAddress.values().iterator();
                        }
                        InetSocketAddress toSendTo = servers.next();
                        while (toSendTo.getPort() == leader.getMyPort() || toSendTo.getPort() == GATEWAYADDRESS.getPort()) {
                            toSendTo = servers.next();
                        }
                        sendWork(message, toSendTo);
                        //requestToClientAddress.put(requestID, new InetSocketAddress(message.getSenderHost(), message.getSenderPort()));
                        break;
                    case COMPLETED_WORK:
                        sendResponse(message.getMessageContents(), message.getRequestID());
                        //leader.sendMessage(message.getMessageType(), message.getMessageContents(), GATEWAYADDRESS);
                        //requestToClientAddress.remove(message.getRequestID());
                        break;
                }

            } else {
                try {
                    Thread.sleep(10);
                } catch (Exception e){}
            }
            checkUDPQueue();
        }
    }

    private void sendResponse(byte[] result, long requestID) {
        Message work = new Message(Message.MessageType.COMPLETED_WORK,
                result,
                leader.getMyAddress().getHostName(),
                leader.getMyAddress().getPort(),
                GATEWAYADDRESS.getHostName(),
                GATEWAYADDRESS.getPort(),
                requestID);
        outgoingMessagesTCP.offer(work);
    }

    private void sendWork(Message message, InetSocketAddress worker) {
        Message work = new Message(message.getMessageType(),
                message.getMessageContents(),
                leader.getMyAddress().getHostName(),
                leader.getMyAddress().getPort(),
                worker.getHostName(),
                worker.getPort(),message.getRequestID());
        outgoingMessagesTCP.offer(work);
    }

    private void checkUDPQueue() {
        if(incomingMessagesUDP.peek() != null) {
            Message message = incomingMessagesUDP.poll();
            //System.out.println(workerServer.getMyPort() + " got " + message.getMessageType() + " message from " + message.getSenderPort());
            switch (message.getMessageType()) {
                case ELECTION:
                    String electionResponse =
                            leader.getCurrentLeader().getCandidateID() + " " +
                                    leader.getPeerState() + " " +
                                    leader.getId() + " " +
                                    leader.getPeerEpoch();
                    InetSocketAddress senderAddress = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
                    leader.sendMessage(Message.MessageType.ELECTION, electionResponse.getBytes(), senderAddress);
                    break;
                default:
                    incomingMessagesUDP.offer(message);
                    break;
            }
        }
    }

    void shutdown() {
        shutdown = true;

    }
}