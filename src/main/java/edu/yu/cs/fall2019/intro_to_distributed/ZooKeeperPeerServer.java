package edu.yu.cs.fall2019.intro_to_distributed;

import java.net.InetSocketAddress;

public interface ZooKeeperPeerServer extends Runnable
{
    void shutdown();

    @Override
    void run();

    void setCurrentLeader(Vote v);

    Vote getCurrentLeader();

    void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException;

    void sendBroadcast(Message.MessageType type, byte[] messageContents);

    ServerState getPeerState();

    void setPeerState(ServerState newState);

    Long getId();

    long getPeerEpoch();

    InetSocketAddress getMyAddress();

    int getMyPort();

    InetSocketAddress getPeerByID(long id);

    int getQuorumSize();

    enum ServerState
    {
        LOOKING, FOLLOWING, LEADING, OBSERVING;
        public char getChar()
        {
            switch(this){
                case LOOKING:
                    return 'O';
                case LEADING:
                    return 'E';
                case FOLLOWING:
                    return 'F';
                case OBSERVING:
                    return 'B';
            }
            return 'z';
        }
        public static ZooKeeperPeerServer.ServerState getServerState(char c)
        {
            switch(c){
                case 'O':
                    return LOOKING;
                case 'E':
                    return LEADING;
                case 'F':
                    return FOLLOWING;
                case 'B':
                    return OBSERVING;
            }
            return null;
        }
    }
}