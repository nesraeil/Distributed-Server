package edu.yu.cs.fall2019.intro_to_distributed;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class UDPMessageReceiver implements Runnable
{
    private static final int MAXLENGTH = 4096;
    private final InetSocketAddress myAddress;
    private final int myPort;
    private LinkedBlockingQueue<Message> incomingMessages;
    private volatile boolean shutdown = false;
    LinkedBlockingQueue<Message> incomingHeartGossip;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;


    public UDPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages,
                              LinkedBlockingQueue<Message> incomingHeartGossip,
                              ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress,
                              InetSocketAddress myAddress,int myPort)
    {
        this.incomingMessages = incomingMessages;
        this.incomingHeartGossip = incomingHeartGossip;
        this.myAddress = myAddress;
        this.myPort = myPort;
        this.peerIDtoAddress = peerIDtoAddress;
    }

    public void shutdown()
    {
        this.shutdown = true;
    }

    @Override
    public void run()
    {
        //create the socket
        DatagramSocket socket = null;
        try
        {
            socket = new DatagramSocket(this.myAddress);
            socket.setSoTimeout(3000);
        }
        catch(Exception e)
        {
            System.err.println("failed to create receiving socket");
            e.printStackTrace();
        }
        //loop
        while (!this.shutdown)
        {
            try
            {
                DatagramPacket packet = new DatagramPacket(new byte[MAXLENGTH], MAXLENGTH);
                socket.receive(packet); // Receive packet from a client
                Message received = new Message(packet.getData());
                //Ignores all messages from servers that are in the dead list
                //if(isSenderAlive(received.getSenderHost(), received.getSenderPort())) {
                    if(received.getMessageType() == Message.MessageType.HEARTBEAT || received.getMessageType() == Message.MessageType.GOSSIP) {
                        this.incomingHeartGossip.put(received);
                    } else {
                        this.incomingMessages.put(received);
                    }
                //}

            }
            catch(SocketTimeoutException ste)
            {
            }
            catch (Exception e)
            {
                if (!this.shutdown)
                {
                    e.printStackTrace();
                }
            }
        }
        //cleanup
        if(socket != null)
        {
            socket.close();
        }
    }

    //If the server that we got a message from is not in the serverList, return false
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