package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;

class JavaRunnerFollower
{
    private ZooKeeperPeerServer workerServer;
    private LinkedBlockingQueue<Message> incomingMessagesTCP;
    private LinkedBlockingQueue<Message> incomingMessagesUDP;
    private LinkedBlockingQueue<Message> outgoingMessagesTCP;
    private LinkedBlockingQueue<Message> outgoingMessagesUDP;

    private JavaRunnerImpl javaRunner;
    private boolean shutdown;

    JavaRunnerFollower(ZooKeeperPeerServer workerServer ,
                       LinkedBlockingQueue<Message> incomingMessagesTCP,
                       LinkedBlockingQueue<Message> outgoingMessagesTCP,
                       LinkedBlockingQueue<Message> incomingMessagesUDP,
                       LinkedBlockingQueue<Message> outgoingMessagesUDP) {
        this.workerServer = workerServer;
        this.incomingMessagesTCP = incomingMessagesTCP;
        this.outgoingMessagesTCP = outgoingMessagesTCP;
        this.incomingMessagesUDP = incomingMessagesUDP;
        this.outgoingMessagesUDP = outgoingMessagesUDP;
        javaRunner = new JavaRunnerImpl();
        shutdown = false;
    }

    void start() {
        while(!shutdown) {
            if(incomingMessagesTCP.peek() != null) {


                Message message = incomingMessagesTCP.poll();
                //System.out.println(workerServer.getMyPort() + " got " + message.getMessageType() + " message from " + message.getSenderPort());
                switch (message.getMessageType()) {
                    case WORK:
                        try {
                            String result = javaRunner.compileAndRun(new ByteArrayInputStream(message.getMessageContents()));
                            long leader = workerServer.getCurrentLeader().getCandidateID();
                            sendResponse(result,leader, message.getRequestID());
                        } catch (IOException e) {
                            //TODO
                        }
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

    private void checkUDPQueue() {
        if(incomingMessagesUDP.peek() != null) {
            Message message = incomingMessagesUDP.poll();
            //System.out.println(workerServer.getMyPort() + " got " + message.getMessageType() + " message from " + message.getSenderPort());
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

    private void sendResponse(String result, long leader, long requestID) {
        Message work = new Message(Message.MessageType.COMPLETED_WORK,
                result.getBytes(),
                workerServer.getMyAddress().getHostName(),
                workerServer.getMyAddress().getPort(),
                workerServer.getPeerByID(leader).getHostName(),
                workerServer.getPeerByID(leader).getPort(),
                requestID);
        outgoingMessagesTCP.offer(work);
    }

    void shutdown() {
        shutdown = true;
    }
}