package edu.yu.cs.fall2019.intro_to_distributed;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

public class UDPMessageReceiver implements Runnable
{
    private static final int MAXLENGTH = 4096;
    private final InetSocketAddress myAddress;
    private final int myPort;
    private LinkedBlockingQueue<Message> incomingMessages;
    private volatile boolean shutdown = false;
    LinkedBlockingQueue<Message> incomingHeartGossip;
    HashSet<String> deadServers;

    public UDPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages,
                              LinkedBlockingQueue<Message> incomingHeartGossip,
                              HashSet<String> deadServers,
                              InetSocketAddress myAddress,int myPort)
    {
        this.incomingMessages = incomingMessages;
        this.incomingHeartGossip = incomingHeartGossip;
        this.myAddress = myAddress;
        this.myPort = myPort;
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
                if(!deadServers.contains(received.getSenderHost() + received.getSenderPort())) {
                    if(received.getMessageType() == Message.MessageType.HEARTBEAT || received.getMessageType() == Message.MessageType.GOSSIP) {
                        this.incomingHeartGossip.put(received);
                    } else {
                        this.incomingMessages.put(received);
                    }
                }

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
}