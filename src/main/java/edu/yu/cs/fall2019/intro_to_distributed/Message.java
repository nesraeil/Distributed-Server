package edu.yu.cs.fall2019.intro_to_distributed;

import java.nio.ByteBuffer;

public class Message
{
    public enum MessageType
    {
        ELECTION,WORK,COMPLETED_WORK;
        public char getChar()
        {
            switch(this){
                case ELECTION:
                    return 'E';
                case WORK:
                    return 'W';
                case COMPLETED_WORK:
                    return 'C';
            }
            return 'z';
        }
        public static MessageType getType(char c)
        {
            switch(c){
                case 'E':
                    return ELECTION;
                case 'W':
                    return WORK;
                case 'C':
                    return COMPLETED_WORK;
            }
            return null;
        }
    };
    private MessageType messageType;
    private String senderHost;
    private int senderPort;
    private String receiverHost;
    private int receiverPort;
    private byte[] contents;
    private byte[] networkPayload;
    private long requestID = -1;

    public Message(MessageType type, byte[] contents, String senderHost, int senderPort, String receiverHost, int receiverPort)
    {
        this.contents = contents;
        this.messageType = type;
        this.senderHost = senderHost;
        this.receiverHost = receiverHost;
        this.senderPort = senderPort;
        this.receiverPort = receiverPort;
    }
    public Message(MessageType type, byte[] contents, String senderHost, int senderPort, String receiverHost, int receiverPort,long requestID)
    {
        this(type,contents, senderHost, senderPort, receiverHost, receiverPort);
        this.requestID = requestID;
        this.getNetworkPayload();
    }
    public long getRequestID()
    {
        return this.requestID;
    }
    public byte[] getMessageContents()
    {
        return this.contents;
    }
    public MessageType getMessageType()
    {
        return this.messageType;
    }
    public String getSenderHost()
    {
        return this.senderHost;
    }

    public int getSenderPort()
    {
        return this.senderPort;
    }

    public String getReceiverHost()
    {
        return this.receiverHost;
    }

    public int getReceiverPort()
    {
        return this.receiverPort;
    }

    public byte[] getNetworkPayload()
    {
        if(this.networkPayload != null)
        {
            return this.networkPayload;
        }
        /*
        size of buffer =
        1 char (msg type) = 2 bytes
        1 long (request ID) = 8 bytes
        1 int for send port = 4 bytes
        1 int to give count of chars in sender host address = 4 bytes
        1 string of sender host address = length in bytes
        1 int for send port = 4 bytes
        1 int to give count of chars in receiver host address = 4 bytes
        1 string of receiver host address = length in bytes
        1 int to give count of content length = 4 bytes
        byte[] of actual message content
         = 30 + string lengths and content lengths
         */
        byte[] senderHostBytes = this.senderHost.getBytes();
        byte[] receiverHostBytes = this.receiverHost.getBytes();
        int bufferSize = 30 + senderHostBytes.length + receiverHostBytes.length + this.contents.length;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putChar(this.messageType.getChar());
        buffer.putLong(this.requestID);
        buffer.putInt(this.senderPort);
        buffer.putInt(senderHostBytes.length);
        buffer.put(senderHostBytes);
        buffer.putInt(this.receiverPort);
        buffer.putInt(receiverHostBytes.length);
        buffer.put(receiverHostBytes);
        buffer.putInt(this.contents.length);
        buffer.put(this.contents);
        buffer.flip();
        this.networkPayload = buffer.array();
        return this.networkPayload;
    }

    /**
     * create message from network payload
     * @param networkPayload
     */
    public Message(byte[] networkPayload)
    {
        this.networkPayload = networkPayload;
        ByteBuffer buffer = ByteBuffer.wrap(this.networkPayload);
        buffer.clear();
        this.messageType = MessageType.getType(buffer.getChar());
        this.requestID = buffer.getLong();
        this.senderPort = buffer.getInt();
        //sender host address
        int hostLength = buffer.getInt(); //address length
        byte[] hostBytes = new byte[hostLength];
        buffer.get(hostBytes); //address bytes
        this.senderHost = new String(hostBytes);
        this.receiverPort = buffer.getInt();
        //receiver host address
        hostLength = buffer.getInt();
        hostBytes = new byte[hostLength];
        buffer.get(hostBytes);
        this.receiverHost = new String(hostBytes);
        //message contents
        int messageLength = buffer.getInt(); //content length
        byte[] messageContents = new byte[messageLength];
        buffer.get(messageContents); //contents
        this.contents = messageContents;
    }
}