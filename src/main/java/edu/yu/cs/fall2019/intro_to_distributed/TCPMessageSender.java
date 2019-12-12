package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.OutputStream;
import java.net.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TCPMessageSender implements Runnable
{
    private LinkedBlockingQueue<Message> outgoingMessages;
    private volatile boolean shutdown = false;

    public TCPMessageSender(LinkedBlockingQueue<Message> outgoingMessages)
    {
        this.outgoingMessages = outgoingMessages;
    }
    public void shutdown()
    {
        this.shutdown = true;
    }
    @Override
    public void run()
    {
        while (!this.shutdown)
        {
            try
            {
                Message messageToSend = this.outgoingMessages.poll(2, TimeUnit.SECONDS);
                if(messageToSend != null)
                {
                    Socket socket= new Socket(messageToSend.getReceiverHost(), messageToSend.getReceiverPort());
                    OutputStream os =  socket.getOutputStream();
                    os.write(messageToSend.getNetworkPayload());
                    os.close();
                    socket.close();
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}