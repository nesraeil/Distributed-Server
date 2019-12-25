package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

public class Heartbeat implements Runnable {

    private ZooKeeperPeerServer myServer;
    private LinkedBlockingQueue<Message> incomingHeartbeat;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;
    private HashMap<Long, HeartbeatData> serverTracker;
    private boolean shutdown;
    private int beatDelay;
    private int TFAIL;
    private int TCLEANUP;
    private long myHeartbeat;
    private long clockTimer;
    private ScheduledExecutorService heart;
    private ScheduledExecutorService clock;



    Heartbeat(ZooKeeperPeerServer myServer ,
              LinkedBlockingQueue<Message> incomingHeartGossip,
              ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress) {
        this.myServer = myServer;
        this.incomingHeartbeat = incomingHeartGossip;
        this.peerIDtoAddress = peerIDtoAddress;
        serverTracker = new HashMap<>();
        //initServerTracker();
        this.shutdown = false;

        beatDelay = 2;
        TFAIL = beatDelay * 3;
        TCLEANUP = TFAIL * 2;
        myHeartbeat = 0;
        clockTimer = 0;

        heart = Executors.newSingleThreadScheduledExecutor();
        clock = Executors.newSingleThreadScheduledExecutor();
    }
    @Override
    public void run() {
        startClock();
        startBeating();
        while (!shutdown) {
            Message m = null;
            try {
                m = incomingHeartbeat.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {}
            if(m == null) continue;

            if(m.getMessageType() == Message.MessageType.HEARTBEAT) {
                updateByHeartbeat(m);
            } else {
                updateByGossip(m);
            }
            checkAllServers();
        }
    }

    private void checkAllServers() {
        for(Long key: serverTracker.keySet()) {
            long elapsedTime = myHeartbeat - serverTracker.get(key).receivedTime;
            if(elapsedTime >= TCLEANUP) {
                removeServer(key);
            } else if(elapsedTime >= TFAIL) {
                serverTracker.get(key).failed = true;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void updateByGossip(Message m) {
        //Converting bytearray from message back to hashmap of heartbeat data
        HashMap<Long, HeartbeatData> toMerge = null;
        ByteArrayInputStream byteIn = new ByteArrayInputStream(m.getMessageContents());
        try {
            ObjectInputStream objIn = new ObjectInputStream(byteIn);
            toMerge = (HashMap<Long, HeartbeatData>)objIn.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        for(Long mergeKey: toMerge.keySet()) {
            //if incoming heartbeat data line is not already marked as failed...
            // and its time for this line is greater than the time that I have for it
            try {
                updateTracker(mergeKey, toMerge.get(mergeKey).otherHeartbeat);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


    private void updateByHeartbeat(Message m) {
        //Getting the beat value from message
        String[] contents = new String(m.getMessageContents()).split(" ");
        Long senderHeartbeat = Long.parseLong(contents[0]);
        Long senderID = Long.parseLong(contents[1]);

        //Updating server's heartbeat info in the tracker
        updateTracker(senderID, senderHeartbeat);
    }


    //Updates the heartbeat at id in server tracker
    private void updateTracker(Long id, Long newHeartbeat) {
        if(!serverTracker.containsKey(id)) {
            //If we dont have any info on this server yet
            serverTracker.put(id, new HeartbeatData(id, newHeartbeat, clockTimer));
        } else {
            //Otherwise, update the info that we already have (if server is not marked as failed, and new heartbeat is higher than old one)
            HeartbeatData toUpdate = serverTracker.get(id);
            if(!toUpdate.failed && newHeartbeat > toUpdate.otherHeartbeat) {
                toUpdate.otherHeartbeat = newHeartbeat;
                toUpdate.receivedTime = clockTimer;
                serverTracker.put(id, toUpdate);
            }

        }

    }

    private void startClock() {
        clock.scheduleAtFixedRate(() -> {
            clockTimer++;
            //System.out.println(myServer.getMyPort() + "heartbeat: " + clockTimer);
        }, 0, 1, TimeUnit.SECONDS);
    }



    /**
     * Sends heartbeat message every 'beatdelay' milliseconds
     */
    private void startBeating() {
        heart.scheduleAtFixedRate(() -> {
            myHeartbeat++;
            //Sending current time and server ID in space delimited string
            String message = myHeartbeat + " " + myServer.getId();
            myServer.sendBroadcast(Message.MessageType.HEARTBEAT, message.getBytes());
            //Sends gossip data every three beats
            if(myHeartbeat % 3 == 0) {
                try{
                    //Writing server tracker to byte array for message
                    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                    ObjectOutputStream out = new ObjectOutputStream(byteOut);
                    out.writeObject(serverTracker);
                    byteOut.close();
                    out.close();

                    //Sends gossip map to a random server
                    myServer.sendMessage(Message.MessageType.GOSSIP,byteOut.toByteArray(), peerIDtoAddress.get(getRandomServerID()));
                } catch (Exception e) {
                    System.err.println(myServer.getMyPort() + "is unable to send gossip at " + myHeartbeat + " vector time");
                    e.printStackTrace();
                }
            }
        }, 0, beatDelay, TimeUnit.SECONDS);
    }


    void shutdown() {
        shutdown = true;
        heart.shutdown();
        clock.shutdown();
    }

    //This looks kind of scary. Gets a random server ID from peerIDtoAddress map
    private Long getRandomServerID() {
        int random = ThreadLocalRandom.current().nextInt(0, peerIDtoAddress.size());
        return (long)peerIDtoAddress.keySet().toArray()[random];

    }

    private void removeServer(Long id) {
        peerIDtoAddress.remove(id);
        serverTracker.remove(id);
    }

    /*private void initServerTracker() {
        for(Long id: peerIDtoAddress.keySet()) {
            serverTracker.put(id, new HeartbeatData(id, 0L, 0L));
        }
    }*/


    static class HeartbeatData implements Serializable {
        final long otherServerID;
        long otherHeartbeat;
        long receivedTime;
        boolean failed;

        HeartbeatData(long otherServerID, long otherHeartbeat, long receivedTime) {
            this.otherServerID = otherServerID;
            this.otherHeartbeat = otherHeartbeat;
            this.receivedTime = receivedTime;
            this.failed = false;
        }

    }
}
