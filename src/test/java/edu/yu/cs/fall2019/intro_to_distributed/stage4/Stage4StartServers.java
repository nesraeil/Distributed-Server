package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import edu.yu.cs.fall2019.intro_to_distributed.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Stage4StartServers
{
    private int[] ports = {8000, 8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
    private int GATEWAYPORT = Config.GTWYINTRNL;//Hardcoded
    private ArrayList<ZooKeeperPeerServer> servers;

    public Stage4StartServers() throws Exception{
        //Step 1: Create Servers
        createServers();

        //step2.1: wait for election to happen
        try
        {
            Thread.sleep(4000);
        }
        catch (Exception e)
        {
        }
        printLeaders();
    }

    //The unsafe cast was supplied by prof. diament, and I did not see another way to do it
    @SuppressWarnings("unchecked")
    private void createServers()
    {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < this.ports.length; i++)
        {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
        }

        //create servers
        this.servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet())
        {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            if(entry.getValue().getPort() == GATEWAYPORT) {
                ZooKeeperPeerServer server = new Gateway(entry.getValue().getPort(), 0, entry.getKey(), map);
                servers.add(server);
                new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
            } else {
                ZooKeeperPeerServer server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
                servers.add(server);
                new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
            }
        }


    }

    private void printLeaders()
    {
        for (ZooKeeperPeerServer server : this.servers)
        {
            Vote leader = server.getCurrentLeader();
            if (leader != null)
            {
                System.out.println("Server on port " + server.getMyAddress().getPort() + " whose ID is " + server.getId() + " has the following ID as its leader: " + leader.getCandidateID() + " and its state is " + server.getPeerState().name());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Stage4StartServers();
    }
}