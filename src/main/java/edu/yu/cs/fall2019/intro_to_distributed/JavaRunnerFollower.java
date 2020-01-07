package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class JavaRunnerFollower
{
    private final long leader;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ZooKeeperPeerServer workerServer;
    private LinkedBlockingQueue<Message> incomingMessagesTCP;
    private LinkedBlockingQueue<Message> incomingMessagesUDP;
    private LinkedBlockingQueue<Message> outgoingMessagesTCP;
    private LinkedBlockingQueue<Message> outgoingMessagesUDP;

    private JavaRunnerImpl javaRunner;
    private boolean shutdown;

    JavaRunnerFollower(ZooKeeperPeerServer workerServer,
                       LinkedBlockingQueue<Message> incomingMessagesTCP,
                       LinkedBlockingQueue<Message> outgoingMessagesTCP,
                       LinkedBlockingQueue<Message> incomingMessagesUDP,
                       LinkedBlockingQueue<Message> outgoingMessagesUDP,
                       ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress) {
        this.workerServer = workerServer;
        this.incomingMessagesTCP = incomingMessagesTCP;
        this.outgoingMessagesTCP = outgoingMessagesTCP;
        this.incomingMessagesUDP = incomingMessagesUDP;
        this.outgoingMessagesUDP = outgoingMessagesUDP;
        javaRunner = new JavaRunnerImpl();
        shutdown = false;
        this.peerIDtoAddress = peerIDtoAddress;
        this.leader = workerServer.getCurrentLeader().getCandidateID();
    }

    void start() {
        while(!shutdown) {
            if(leaderIsDead()) {
                //If leader is dead, start election
                workerServer.setPeerState(ZooKeeperPeerServer.ServerState.LOOKING);
                shutdown();
            }
            Message message = null;
            try {
                 message = incomingMessagesTCP.poll(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {}
            if(message != null) {
                switch (message.getMessageType()) {
                    case WORK:
                        try {
                            String result = javaRunner.compileAndRun(new ByteArrayInputStream(message.getMessageContents()));
                            if(leaderIsDead()) {
                                //If there is no leader, put the message back into my queue and start election
                                incomingMessagesTCP.offer(message);
                                workerServer.setPeerState(ZooKeeperPeerServer.ServerState.LOOKING);
                                shutdown();
                            } else {
                                sendResponse(result,leader, message.getRequestID());
                            }
                        } catch (IOException e) {
                            incomingMessagesTCP.offer(message);
                        }
                        break;
                }
            }
            checkUDPQueue();
        }

    }

    private boolean leaderIsDead() {
        return !peerIDtoAddress.containsKey(leader);
    }

    private void checkUDPQueue() {
        if(incomingMessagesUDP.peek() != null) {
            Message message = incomingMessagesUDP.poll();
            switch (message.getMessageType()) {
                case ELECTION:
                    String electionResponse =
                            workerServer.getCurrentLeader().getCandidateID() + " " +
                                    workerServer.getPeerState() + " " +
                                    workerServer.getId() + " " +
                                    workerServer.getPeerEpoch();
                    InetSocketAddress senderAddress = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
                    workerServer.sendMessage(Message.MessageType.ELECTION, electionResponse.getBytes(), senderAddress);
                    break;
                default:
                    incomingMessagesUDP.offer(message);
                    break;
            }
        }
    }

    private void sendResponse(String result, long leader, long requestID) throws IOException {
        Message work = new Message(Message.MessageType.COMPLETED_WORK,
                result.getBytes(),
                workerServer.getMyAddress().getHostName(),
                workerServer.getMyAddress().getPort(),
                workerServer.getPeerByID(leader).getHostName(),
                workerServer.getPeerByID(leader).getPort(),
                requestID);

        Socket socket= new Socket(work.getReceiverHost(), work.getReceiverPort());
        OutputStream os =  socket.getOutputStream();
        os.write(work.getNetworkPayload());
        os.close();
        socket.close();
        //outgoingMessagesTCP.offer(work);
    }

    void shutdown() {
        shutdown = true;
    }
}