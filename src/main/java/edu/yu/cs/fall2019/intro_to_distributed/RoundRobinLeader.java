package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


class RoundRobinLeader {
    //Hardcoded gateway address
    private InetSocketAddress GATEWAYADDRESS = new InetSocketAddress("localhost", Config.GTWYINTRNL);

    private ZooKeeperPeerServer leader;
    private LinkedBlockingQueue<Message> incomingMessagesTCP;
    private LinkedBlockingQueue<Message> outgoingMessagesTCP;
    private LinkedBlockingQueue<Message> incomingMessagesUDP;
    private LinkedBlockingQueue<Message> outgoingMessagesUDP;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;

    private Iterator<Long> servers;
    private boolean shutdown;

    private HashMap<Long, Set<Message>> sentWork;// Map of server IDs, values are sets with the corresponding messages

    RoundRobinLeader(ZooKeeperPeerServer leader ,
                     LinkedBlockingQueue<Message> incomingMessagesTCP,
                     LinkedBlockingQueue<Message> outgoingMessagesTCP,
                     LinkedBlockingQueue<Message> incomingMessagesUDP,
                     LinkedBlockingQueue<Message> outgoingMessagesUDP,
                     ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress) {
        this.leader = leader;
        this.incomingMessagesTCP = incomingMessagesTCP;
        this.outgoingMessagesTCP = outgoingMessagesTCP;
        this.incomingMessagesUDP = incomingMessagesUDP;
        this.outgoingMessagesUDP = outgoingMessagesUDP;

        this.peerIDtoAddress = peerIDtoAddress;
        servers = peerIDtoAddress.keySet().iterator();
        shutdown = false;

        sentWork = new HashMap<>();
    }

    void start() {
        while(!shutdown) {
            Message message = null;
            try {
                message = incomingMessagesTCP.poll(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(message != null) {
                switch (message.getMessageType()) {
                    case WORK://Sending work to peer servers on round robin basis
                        //If the iterator is done, restart
                        if(!servers.hasNext()) {
                            servers = peerIDtoAddress.keySet().iterator();

                        }
                        long toSendWork = servers.next();
                        //Makes sure that I do not send work to myself, the gateway, or a dead server
                        while (toSendWork == leader.getId() ||
                                peerIDtoAddress.get(toSendWork).getPort() == GATEWAYADDRESS.getPort() ||
                                !peerIDtoAddress.containsKey(toSendWork)) {
                            //If the iterator is done, restart
                            if(!servers.hasNext()) {
                                servers = peerIDtoAddress.keySet().iterator();

                            }
                            toSendWork = servers.next();
                        }
                        sendWork(message, peerIDtoAddress.get(toSendWork));
                        addToSentWorkList(message, toSendWork);
                        break;
                    case COMPLETED_WORK:
                        sendResponse(message.getMessageContents(), message.getRequestID());
                        removeFromSentWorkList(message, getServerIDByPort(message.getSenderPort()));
                        break;
                }

            }
            checkUDPQueue();
            resendDeadServerWork();

        }
    }

    private long getServerIDByPort(int port) {
        for(long serverID :peerIDtoAddress.keySet()) {
            if(peerIDtoAddress.get(serverID).getPort() == port) {
                return serverID;
            }
        }
        return -1;//Server doesn't exist
    }

    private void addToSentWorkList(Message m, long receiverID) {
        if(sentWork.containsKey(receiverID)) {
            sentWork.get(receiverID).add(m);
        } else {
            HashSet<Message> newSet = new HashSet<>();
            newSet.add(m);
            sentWork.put(receiverID, newSet);
        }
    }

    private void removeFromSentWorkList(Message m, long senderID) {

        //Loooks through set at senderID. If message with matching messageID is found, remove from set
        if(sentWork.containsKey(senderID)) {
            Message toRemove = null;
            for (Message setMessage:sentWork.get(senderID)) {
                if(setMessage.getRequestID() == m.getRequestID()) {
                    toRemove = setMessage;
                    break;
                }
            }
            if(toRemove != null) sentWork.get(senderID).remove(toRemove);
        }
    }

    private void resendDeadServerWork() {
        ArrayList<Long> resentWork = new ArrayList<>();
        Set<Message> toResend= new HashSet<>();

        //Finding messages that were sent to now-dead servers
        for(long workerID:sentWork.keySet()) {
            if(!peerIDtoAddress.containsKey(workerID)) {
                toResend.addAll(sentWork.get(workerID));
                resentWork.add(workerID);
            }
        }
        //Adding messages to incoming queue
        for (Message m: toResend) {
            outgoingMessagesTCP.offer(m);
        }
        //Removing messages from sentWork list
        for(long toRemove: resentWork) {
            sentWork.remove(toRemove);
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

    private void sendWork(Message m, InetSocketAddress worker) {
        Message work = new Message(m.getMessageType(),
                m.getMessageContents(),
                leader.getMyAddress().getHostName(),
                leader.getMyAddress().getPort(),
                worker.getHostName(),
                worker.getPort(),m.getRequestID());

        try {
            Socket socket= new Socket(work.getReceiverHost(), work.getReceiverPort());
            OutputStream os =  socket.getOutputStream();
            os.write(work.getNetworkPayload());
            os.close();
            socket.close();
        } catch (IOException e) {
            //If unable to send, that means the server is dead
            //and the leader will resend the message when it finds out
        }
        //outgoingMessagesTCP.offer(work);
    }

    private void checkUDPQueue() {
        if(incomingMessagesUDP.peek() != null) {
            Message message = incomingMessagesUDP.poll();
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