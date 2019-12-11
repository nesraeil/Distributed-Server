package edu.yu.cs.fall2019.intro_to_distributed;

public class ElectionNotification
{
    //Proposed leader
    public long leader;
    //current state of sender
    public ZooKeeperPeerServer.ServerState state;
    //ID of sender
    public long sid;
    //epoch of the proposed leader
    public long peerEpoch;

    public ElectionNotification(long leader, ZooKeeperPeerServer.ServerState state, long sid, long peerEpoch)
    {
        this.leader = leader;
        this.state = state;
        this.sid = sid;
        this.peerEpoch = peerEpoch;
    }
}