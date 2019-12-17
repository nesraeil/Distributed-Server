package edu.yu.cs.fall2019.intro_to_distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer.ServerState.*;

public class ZooKeeperLeaderElection {
    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServer myPeerServer;
    private long proposedLeader;
    private long proposedEpoch;

    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.proposedLeader = server.getId();
        this.proposedEpoch = server.getPeerEpoch();
    }

    public synchronized Vote getVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader() {
        //send initial notifications to other peers to get things started
        sendNotifications();
        //Loop, exchanging notifications with other servers until we find a leader
        Map<Long, Vote> votes = new HashMap<>();
        while (this.myPeerServer.getPeerState() == LOOKING || this.myPeerServer.getPeerState() == OBSERVING) {

            //Remove next notification from queue, timing out after 2 times the termination time
            Message m = messageBackoff();

            //if/when we get a message (removed: and it's from a valid server and for a valid server...)
            if(m != null && m.getMessageType() == Message.MessageType.ELECTION) {//GET BACK TO THIS
                ElectionNotification electNoti = messageToElectNoti(m);
                switch (electNoti.state) {
                    //switch on the state of the sender:
                    case LOOKING: //if the sender is also looking
                        //if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is...
                        if (newVoteSupersedesCurrent(electNoti.leader, electNoti.peerEpoch, proposedLeader, proposedEpoch)) {
                            proposedLeader = electNoti.leader;
                            proposedEpoch = electNoti.peerEpoch;
                            sendNotifications();
                        }
                        //...while keeping track of the votes I received and who I received them from
                        Vote messageVote = new Vote(electNoti.leader, electNoti.peerEpoch);

                        votes.put(electNoti.sid, messageVote);

                        //if I have enough votes to declare a leader:
                        Vote myCurrentVote = getVote();
                        if (haveEnoughVotes(votes, myCurrentVote)) {
                            //check if there are any new votes for a higher ranked possible leader before I declare a leader. If so,continue in my election Loop
                            while (incomingMessages.peek() != null) {
                                if (messageToElectNoti(incomingMessages.peek()).leader > proposedLeader) {
                                    continue;
                                } else {
                                    incomingMessages.poll();
                                }
                            }
                            //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone else won the election) and exit the election
                            return acceptElectionWinner(electNoti);
                        }
                        break;
                    case FOLLOWING:
                    case LEADING: //if the sender is following a leader already or thinks it is the leader
                        //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. give
                        // the majority to some peer among the set of votes in my epoch.
                        Vote newVote = new Vote(electNoti.leader, electNoti.peerEpoch);
                        votes.put(electNoti.sid, newVote);

                        //if so, accept the election winner. I don't count who voted for who, since as I receive them I will
                        // automatically change my vote to the highest sid, as will everyone else.
                        //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                        //ELSE: if n is from a later election epoch and/or there are not enough votes in my epoch...
                        //...before joining their established ensemble, verify that a majority are following the same leader.from that epoch
                        //if so, accept their leader. If not, keep looping on the election loop.
                        if (haveEnoughVotes(votes, newVote) ||
                                (newVote.getPeerEpoch() >= proposedEpoch && haveEnoughVotes(mapOfNewestEpochVotes(votes, electNoti), newVote))) {
                            proposedEpoch = electNoti.peerEpoch;
                            proposedLeader = electNoti.leader;
                            return acceptElectionWinner(electNoti);
                        }
                        break;
                }
            }
        }
        return null;
    }

    private ElectionNotification messageToElectNoti(Message m) {
        String[] contents = new String(m.getMessageContents()).split(" ");
        ElectionNotification electNoti = new ElectionNotification(
                Long.parseLong(contents[0]),//Proposed Leader
                ZooKeeperPeerServer.ServerState.valueOf(contents[1]),// Sender's peer state
                Long.parseLong(contents[2]),//Sender's server ID
                Long.parseLong(contents[3]));//Sender's proposed epoch

        return electNoti;
    }

    private Message messageBackoff() {
        Message m = null;
        try {
            m = incomingMessages.poll(maxNotificationInterval*2, TimeUnit.MILLISECONDS);//2* termination time?
            if(m == null) {
                //if no notifications received...
                //...resend notifications to prompt a reply from others...
                //..and implement exponential backoff when notifications not received...
                sendNotifications();
                int backoff = 1;
                while(m == null && backoff <= maxNotificationInterval) {
                    m = incomingMessages.poll(backoff, TimeUnit.MILLISECONDS);
                    backoff *= 2;
                }
            }
        } catch (java.lang.InterruptedException e) {
            e.printStackTrace();
        }
        return m;
    }

    private void sendNotifications() {
        String messageContents =
                proposedLeader + " " +
                myPeerServer.getPeerState() + " " +
                myPeerServer.getId() + " " +
                proposedEpoch;
        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, messageContents.getBytes());
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        //If this server is not an observer, set my state to either LEADING or FOLLOWING
        if (n.leader == myPeerServer.getId()) {
            myPeerServer.setPeerState(LEADING);
        } else if(myPeerServer.getPeerState() != OBSERVING)  {
            myPeerServer.setPeerState(FOLLOWING);
        }
        //clear out the incoming queue before returning

        incomingMessages.clear();

        return getVote();
    }


    /**
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean newVoteSupersedesCurrent(long newId, long newEpoch, long curId, long curEpoch) {
        return (newEpoch > curEpoch) || ((newEpoch == curEpoch) && (newId > curId));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient to declare the end of the election round.
     * I don't count who voted for who, since as I receive them I will automatically change my vote to the highest sid, as will
     * everyone else
     */
    protected boolean haveEnoughVotes(Map<Long, Vote> votes, Vote vote) {
        int inFavor = 0;
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                inFavor++;
            }
        }
        return this.myPeerServer.getQuorumSize() <= inFavor;
    }

    private Map<Long, Vote> mapOfNewestEpochVotes(Map<Long, Vote> map, ElectionNotification e) {
        Map<Long, Vote> result = new HashMap<>();
        for (Map.Entry<Long, Vote> entry : map.entrySet()) {
            if (e.peerEpoch ==  entry.getValue().getPeerEpoch()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
}
