package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPMessageReceiver implements Runnable{
    private LinkedBlockingQueue<Message> incomingMessages;
    private volatile boolean shutdown = false;
    private int myPort;

    public TCPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages, int myPort) {
        this.incomingMessages = incomingMessages;
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
                this.incomingMessages.offer(received);
            }
            catch (Exception e) {
                    if (!this.shutdown)
                    {
                        e.printStackTrace();
                    }
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
}
