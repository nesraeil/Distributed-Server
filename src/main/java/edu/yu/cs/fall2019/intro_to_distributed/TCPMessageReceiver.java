package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPMessageReceiver implements Runnable{
    private LinkedBlockingQueue<Message> incomingMessages;
    private volatile boolean shutdown = false;
    private HashMap<Long,InetSocketAddress> peerIDtoAddress;
    private int myPort;

    public TCPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages,
                              HashMap<Long,InetSocketAddress> peerIDtoAddress,
                              int myPort) {
        this.incomingMessages = incomingMessages;
        this.peerIDtoAddress = peerIDtoAddress;
        this.myPort = myPort;
    }
    public void shutdown()
    {
        this.shutdown = true;
    }
    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(myPort);
            serverSocket.setSoTimeout(3000);
        } catch (Exception e) {
            System.err.println("failed to create receiving socket");
            e.printStackTrace();
        }

        //loop
        while (!this.shutdown) {
            try {
                Socket socket = serverSocket.accept();
                InputStream input = socket.getInputStream();
                Message received = new Message(Util.readAllBytes(input));
                /*if(isSenderAlive(received.getSenderHost(), received.getSenderPort())) {
                    this.incomingMessages.put(received);
                }*/
                this.incomingMessages.offer(received);
            }
            catch (Exception e) {
/*
                    if (!this.shutdown)
                    {
                        e.printStackTrace();
                    }
*/
            }
        }

        //cleanup
        if(serverSocket != null)
        {
            try {
                serverSocket.close();
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private boolean isSenderAlive(String host, int port) {
        String senderServer = host + port;
        for(Long id:peerIDtoAddress.keySet()) {
            String peerServer = peerIDtoAddress.get(id).getHostName() + peerIDtoAddress.get(id).getPort();
            if(peerServer.equals(senderServer)) {
                return true;
            }
        }
        return false;
    }
}
