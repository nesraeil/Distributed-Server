package edu.yu.cs.fall2019.intro_to_distributed;

import edu.yu.cs.fall2019.intro_to_distributed.JavaRunnerImpl;
import edu.yu.cs.fall2019.intro_to_distributed.Message;
import edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

class JavaRunnerFollower
{
    private ZooKeeperPeerServer workerServer;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private JavaRunnerImpl javaRunner;
    private boolean shutdown;

    JavaRunnerFollower(ZooKeeperPeerServer workerServer ,
                              LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages) {
        this.workerServer = workerServer;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        javaRunner = new JavaRunnerImpl();
        shutdown = false;
    }

    void start() {
        while(!shutdown) {
            if(incomingMessages.peek() != null) {
                Message message = incomingMessages.poll();
                //System.out.println(workerServer.getMyPort() + " got " + message.getMessageType() + " message from " + message.getSenderPort());
                switch (message.getMessageType()) {
                    case WORK:
                        try {
                            String result = javaRunner.compileAndRun(new ByteArrayInputStream(message.getMessageContents()));
                            sendResponse(result, message.getSenderHost(), message.getSenderPort(), message.getRequestID());
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
        }
    }

    private void sendResponse(String result, String targetHost, int targetPort, long requestID) {
        Message work = new Message(Message.MessageType.COMPLETED_WORK,
                result.getBytes(),
                workerServer.getMyAddress().getHostName(),
                workerServer.getMyAddress().getPort(),
                targetHost,
                targetPort,
                requestID);
        outgoingMessages.offer(work);
    }

    void shutdown() {
        shutdown = true;
    }
}