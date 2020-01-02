package edu.yu.cs.fall2019.intro_to_distributed;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.*;

public class Heartbeat implements Runnable {


    private final ZooKeeperPeerServer myServer;
    private final LinkedBlockingQueue<Message> incomingHeartbeat;
    private volatile ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private final ConcurrentHashMap<Long, HeartbeatData> serverTracker;
    private volatile int clockTimer;
    private boolean shutdown;
    private final int beatDelay;
    private final int TFAIL;
    private final int TCLEANUP;
    private volatile int myHeartbeat;

    private ScheduledExecutorService heart;
    private ScheduledExecutorService serverChecker;

    Heartbeat(ZooKeeperPeerServer myServer ,
              LinkedBlockingQueue<Message> incomingHeartGossip,
              ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress) {
        this.myServer = myServer;
        this.incomingHeartbeat = incomingHeartGossip;
        this.peerIDtoAddress = peerIDtoAddress;
        this.serverTracker = new ConcurrentHashMap<>();
        this.shutdown = false;

        this.beatDelay = 2;
        this.TFAIL = beatDelay * 3;
        this.TCLEANUP = TFAIL * 2;
        this.myHeartbeat = 0;
        this.clockTimer = 0;

        heart = Executors.newSingleThreadScheduledExecutor();
        serverChecker = Executors.newSingleThreadScheduledExecutor();
    }

    void shutdown() {
        shutdown = true;
        heart.shutdown();
        serverChecker.shutdown();
    }

    @Override
    public void run() {
        beat();
        checkAllServers();
        while (!shutdown) {
            Message m = null;
            try {
                m = incomingHeartbeat.poll(1, TimeUnit.SECONDS);

            } catch (InterruptedException e) {}
            if(m == null) continue;

            if(m.getMessageType() == Message.MessageType.HEARTBEAT) {
                updateByHeartbeat(m);
            } else {
                System.out.println("got gossip");
                updateByGossip(m);
            }
        }
    }

    private void checkAllServers() {
        serverChecker.scheduleAtFixedRate(() -> {
        for(Long serverID: serverTracker.keySet()) {

            //Only check servers that are not me
            if(!serverID.equals(myServer.getId())) {
                long elapsedTime = clockTimer - serverTracker.get(serverID).receivedTime;
                if(elapsedTime >= TCLEANUP) {
                    serverTracker.remove(serverID);
                } else if(elapsedTime >= TFAIL && !serverTracker.get(serverID).failed) {
                    serverTracker.get(serverID).failed = true;
                    peerIDtoAddress.remove(serverID);
                    System.out.println(myServer.getId() + ": No heartbeat from server " + serverID + " - server failed");
                }
            }
        }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }



    private void beat() {
        heart.scheduleAtFixedRate(() -> {
            clockTimer++;
            if(clockTimer % beatDelay == 0) {
                myHeartbeat++;
                //Sending current time and server ID in space delimited string
                String message = myHeartbeat + " " + myServer.getId();
                myServer.sendBroadcast(Message.MessageType.HEARTBEAT, message.getBytes());
                //Sends gossip data every three beats
                if(myHeartbeat % 2 == 0) {
                    Gson gson = new Gson();
                    String jsonString = myServer.getId() + "#" + gson.toJson(serverTracker);
                    myServer.sendMessage(Message.MessageType.GOSSIP, jsonString.getBytes(), getRandomServer());
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }


    class HeartbeatData {
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

    private InetSocketAddress getRandomServer() {
        Random rand = new Random();
        int randInt = rand.nextInt(peerIDtoAddress.size());
        long randomID = (long)peerIDtoAddress.keySet().toArray()[randInt];
        return peerIDtoAddress.get(randomID);
    }

    private void updateByGossip(Message m) {
        //Converting gossip json to hashmap
        String[] contents = new String(m.getMessageContents()).split("#");
        String senderID = contents[0];
        String json = contents[1];
        Gson gson = new Gson();
        Type type = new TypeToken<HashMap<Long, HeartbeatData>>(){}.getType();
        HashMap<Long, HeartbeatData> gossipMap = gson.fromJson(json, type);

        for(long gossipID : gossipMap.keySet()) {
            if(updateServerTracker(gossipID, gossipMap.get(gossipID).otherHeartbeat)) {
                String message = myServer.getId() + ": updated " + gossipID + "'s heartbeat to " + gossipMap.get(gossipID).otherHeartbeat
                        + " based on gossip from " + senderID + " at node time " + clockTimer;
                System.out.println(message);
            }
        }
    }

    private void updateByHeartbeat(Message m) {
        //Getting the beat value from message
        String[] contents = new String(m.getMessageContents()).split(" ");
        long senderHeartbeat = Long.parseLong(contents[0]);
        long senderID = Long.parseLong(contents[1]);

        //Updating server's heartbeat info in the tracker
        updateServerTracker(senderID, senderHeartbeat);
    }

    private boolean updateServerTracker(Long id, Long newHeartbeat) {
        if(!serverTracker.containsKey(id)) {
            //If we dont have any info on this server yet
            serverTracker.put(id, new HeartbeatData(id, 0L, 0L));
        }

        //Update the info that we already have (if server is not marked as failed, and new heartbeat is higher than old one)
        HeartbeatData toUpdate = serverTracker.get(id);
        if(!toUpdate.failed && newHeartbeat > toUpdate.otherHeartbeat && !id.equals(myServer.getId())) {
            toUpdate.otherHeartbeat = newHeartbeat;
            toUpdate.receivedTime = clockTimer;
            serverTracker.put(id, toUpdate);
            return true;
        }
        return false;
    }

}