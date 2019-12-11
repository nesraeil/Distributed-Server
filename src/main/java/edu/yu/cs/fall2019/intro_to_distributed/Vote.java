package edu.yu.cs.fall2019.intro_to_distributed;

public class Vote
{
    final private long candidateID;
    final private long peerEpoch;
    final private ZooKeeperPeerServer.ServerState state;

    public Vote(long candidateID, long peerEpoch)
    {
        this.candidateID = candidateID;
        this.peerEpoch = peerEpoch;
        this.state = ZooKeeperPeerServer.ServerState.LOOKING;
    }

    public Vote(long candidateID, long peerEpoch, ZooKeeperPeerServer.ServerState state)
    {
        this.candidateID = candidateID;
        this.state = state;
        this.peerEpoch = peerEpoch;
    }

    public long getCandidateID()
    {
        return candidateID;
    }

    public long getPeerEpoch()
    {
        return peerEpoch;
    }

    public ZooKeeperPeerServer.ServerState getState()
    {
        return state;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Vote))
        {
            return false;
        }
        Vote other = (Vote) o;

        if ((state == ZooKeeperPeerServer.ServerState.LOOKING) || (other.state == ZooKeeperPeerServer.ServerState.LOOKING))
        {
            return (candidateID == other.candidateID && peerEpoch == other.peerEpoch);
        }
        else
        {
            return (candidateID == other.candidateID && peerEpoch == other.peerEpoch);
        }
    }

    @Override
    public int hashCode()
    {
        return (int) (candidateID);
    }

    public String toString()
    {
        return "(" + candidateID + ", " + Long.toHexString(peerEpoch) + ")";
    }
}

/*
 * There are two things going on in the logic below:
 *
 * 1. skip comparing the zxid and electionEpoch for votes for servers
 *    out of election.
 *
 *    Need to skip those because they can be inconsistent due to
 *    scenarios described in QuorumPeer.updateElectionVote.
 *
 *    And given that only one ensemble can be running at a single point
 *    in time and that each epoch is used only once, using only id and
 *    epoch to compare the votes is sufficient.
 *
 *    {@see https://issues.apache.org/jira/browse/ZOOKEEPER-1805}
 */