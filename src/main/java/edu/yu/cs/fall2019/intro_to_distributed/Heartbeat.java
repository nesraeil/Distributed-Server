package edu.yu.cs.fall2019.intro_to_distributed;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.*;

public class Heartbeat implements Runnable {


    private final ZooKeeperPeerServer myServer;
    private final LinkedBlockingQueue<Message> incomingHeartbeat;
    private volatile ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private final ConcurrentHashMap<Long, HeartbeatData> serverTracker;
    private long clockTimer;
    private boolean shutdown;
    private final int beatDelay;
    private final int TFAIL;
    private final int TCLEANUP;
    private volatile int myHeartbeat;

    private final ScheduledExecutorService heart;
    private final ScheduledExecutorService serverChecker;
    private final ScheduledExecutorService clock;

    private Random rand;

    private ArrayList<String> gossipLog;

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
        clock = Executors.newSingleThreadScheduledExecutor();

        rand  = new Random();
        gossipLog = new ArrayList<>();
    }

    void shutdown() {
        shutdown = true;
        heart.shutdown();
        serverChecker.shutdown();
        clock.shutdown();
    }

    @Override
    public void run() {
        //startClock();
        beat();
        checkAllServers();
        while (!shutdown) {
            Message m = null;
            try {
                m = incomingHeartbeat.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if(m == null) continue;

            if(m.getMessageType() == Message.MessageType.HEARTBEAT) {
                updateByHeartbeat(m);
            } else {
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
                if(beatDelay % 2 == 0) {
                    myServer.sendMessage(Message.MessageType.GOSSIP, serverTrackerToString().getBytes(), getRandomServer());
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private String serverTrackerToString() {
        StringBuilder result = new StringBuilder();
        result.append(myServer.getId()).append(':');
        for(long key: serverTracker.keySet()) {
            result.append(key).append(" ")
                    .append(serverTracker.get(key).otherHeartbeat).append(" ")
                    .append(serverTracker.get(key).receivedTime).append(" ")
                    .append(serverTracker.get(key).failed)
                    .append(',');
        }
        return result.toString();
    }

    private HashMap<Long, HeartbeatData> stringToServerTracker(String input) {
        HashMap<Long, HeartbeatData> result = new HashMap<>();
        String[] entries = input.split(",");
        for(String entryLine: entries) {
            String[] entryArr = entryLine.split(" ");
            long key = Long.parseLong(entryArr[0]);
            long otherHeartbeat = Long.parseLong(entryArr[1]);
            long receivedTime = Long.parseLong(entryArr[2]);
            boolean failed = Boolean.valueOf(entryArr[3]);
            HeartbeatData temp = new HeartbeatData(key, otherHeartbeat, receivedTime, failed);
            result.put(key, temp);
        }
        return result;
    }

    private void updateByGossip(Message m) {
        //Converting gossip json to hashmap
        String contents = new String(m.getMessageContents());
        String[] idAndMap = contents.split(":");
        if(idAndMap.length > 1) {
            gossipLog.add(contents);
            String senderID = idAndMap[0];
            String json = idAndMap[1];
            HashMap<Long, HeartbeatData> gossipMap = stringToServerTracker(json);

            for(long gossipID : gossipMap.keySet()) {
                if(updateServerTracker(gossipID, gossipMap.get(gossipID).otherHeartbeat)) {
                    String message = myServer.getId() + ": updated " + gossipID + "'s heartbeat to " + gossipMap.get(gossipID).otherHeartbeat
                            + " based on gossip from " + senderID + " at node time " + clockTimer;
                    System.out.println(message);
                }
            }
        }
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
        HeartbeatData(long otherServerID, long otherHeartbeat, long receivedTime, boolean failed) {
            this(otherServerID, otherHeartbeat, receivedTime);
            this.failed = failed;
        }
    }

    private InetSocketAddress getRandomServer() {
        int randInt = rand.nextInt(peerIDtoAddress.size());
        long randomID = (long)peerIDtoAddress.keySet().toArray()[randInt];
        return peerIDtoAddress.get(randomID);
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
        if(!id.equals(myServer.getId()) && peerIDtoAddress.containsKey(id)) {
            if(!serverTracker.containsKey(id)) {
                //If we dont have any info on this server yet
                serverTracker.put(id, new HeartbeatData(id, 0L, 0L));
            }

            //Update the info that we already have (if server is not marked as failed, and new heartbeat is higher than old one)
            HeartbeatData toUpdate = serverTracker.get(id);
            if(!toUpdate.failed && newHeartbeat > toUpdate.otherHeartbeat) {
                toUpdate.otherHeartbeat = newHeartbeat;
                toUpdate.receivedTime = clockTimer;
                serverTracker.put(id, toUpdate);
                return true;
            }
        }
        return false;
    }

    ArrayList<String> getGossip() {
        return new ArrayList<>(gossipLog);
    }

}