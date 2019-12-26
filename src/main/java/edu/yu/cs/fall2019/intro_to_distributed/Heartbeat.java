package edu.yu.cs.fall2019.intro_to_distributed;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
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
    private long TFAIL;
    private long TCLEANUP;
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
            //System.out.println(myServer.getId() + " " + clockTimer);
            Message m = null;
            try {
                m = incomingHeartbeat.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {}
            if(m == null) continue;

            if(m.getMessageType() == Message.MessageType.HEARTBEAT) {
                updateByHeartbeat(m);
            } else {
                //updateByGossip(m);
            }
            checkAllServers();
        }
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
                Gson gson = new Gson();
                String jsonString = myServer.getId() + "#" + gson.toJson(serverTracker);
                myServer.sendMessage(Message.MessageType.GOSSIP, jsonString.getBytes(), getRandomServer());
            }

        }, 0, beatDelay, TimeUnit.SECONDS);
    }

    private synchronized void checkAllServers() {
        ArrayList<Long> toCheck =  new ArrayList<>(serverTracker.keySet());
        for(Long serverID: toCheck) {
            long elapsedTime = clockTimer - serverTracker.get(serverID).receivedTime;
            if(elapsedTime > TCLEANUP) {
                removeServer(serverID);
            } else if(elapsedTime > TFAIL) {
                serverTracker.get(serverID).failed = true;
                System.out.println(myServer.getId() + ": No heartbeat from server " + serverID + " - server failed");
                System.out.println(elapsedTime);
            }
        }
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
        Long senderHeartbeat = Long.parseLong(contents[0]);
        Long senderID = Long.parseLong(contents[1]);

        //Updating server's heartbeat info in the tracker
        updateServerTracker(senderID, senderHeartbeat);
    }


    //Updates the heartbeat at key "id" in server tracker
    //Returns true if update is successful
    private boolean updateServerTracker(Long id, Long newHeartbeat) {
        if(!serverTracker.containsKey(id)) {
            //If we dont have any info on this server yet
            serverTracker.put(id, new HeartbeatData(id, 0L, 0L));
        }

        //Update the info that we already have (if server is not marked as failed, and new heartbeat is higher than old one)
        HeartbeatData toUpdate = serverTracker.get(id);
        if(!toUpdate.failed && newHeartbeat > toUpdate.otherHeartbeat && id != myServer.getId()) {
            toUpdate.otherHeartbeat = newHeartbeat;
            toUpdate.receivedTime = clockTimer;
            serverTracker.put(id, toUpdate);
            return true;
        }
        return false;
    }

    private void startClock() {
        clock.scheduleAtFixedRate(() -> {
            clockTimer++;
            //System.out.println(myServer.getMyPort() + "heartbeat: " + clockTimer);
        }, 0, 1, TimeUnit.SECONDS);
    }


    void shutdown() {
        shutdown = true;
        heart.shutdown();
        clock.shutdown();
    }

    //This looks kind of scary. Gets a random server ID from peerIDtoAddress map
    private InetSocketAddress getRandomServer() {
        int randomInt = ThreadLocalRandom.current().nextInt(0, peerIDtoAddress.size());
        long randomID = (long)peerIDtoAddress.keySet().toArray()[randomInt];
        return peerIDtoAddress.get(randomID);
    }

    private void removeServer(Long id) {
        peerIDtoAddress.remove(id);
        serverTracker.remove(id);
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
}
