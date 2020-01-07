package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import edu.yu.cs.fall2019.intro_to_distributed.*;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static edu.yu.cs.fall2019.intro_to_distributed.stage4.Client.*;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Stage4Test
{

    private static ByteArrayOutputStream out;
    private static PrintStream oldOut;


    private static String validClass = "package edu.yu.cs.fall2019.intro_to_distributed.stage1;\n\npublic class HelloWorld\n{\n    public void run()\n    {\n        System.out.print(\"Hello System.out world!\\n\");\n        System.err.print(\"Hello System.err world!\\n\");\n    }\n}\n";
    private static String validClassReponse = "System.err:\n" + "Hello System.err world!\n\n" + "System.out:\n" + "Hello System.out world!\n\n";

    private static int[] ports = {8000, 8010, 8020, 8030, 8040, 8050, 8060, 8070};
    private static int GATEWAYPORT = Config.GTWYINTRNL;//Hardcoded

    private static ArrayList<ZooKeeperPeerServer> servers;
    private static ZooKeeperPeerServer gateway;

    @BeforeClass
    public static void setup() {
        suppressPrints();
        createServers();
    }

    @AfterClass
    public static void cleanup() {
        stopServers();
        resetSystemOut();
    }

    //Making sure that leader is correct
    @Test
    public void test1_checkingLeaders()
    {
        //Assert that the everyone's leader is correct (7 or 6) depending on if servers started too slow
        for(int i = 0; i < ports.length; i++) {
            assertTrue(servers.get(i).getCurrentLeader().getCandidateID() == 7 || servers.get(i).getCurrentLeader().getCandidateID() == 6);
        }
    }

    @Test
    public void test2_validCompileAndRun()
    {
        Response resp = null;
        try {
            resp =  sendHTTP("localhost", Config.GTWYEXTRNL, "compileandrun", validClass);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(resp == null) {
            assert(false);
        } else {
            assertEquals(validClassReponse, resp.getBody());
            assertEquals(200, resp.getCode());
        }
    }

    @Test
    public void test3_badCompileAndRun()
    {
        Response resp = null;
        try {
            resp =  sendHTTP("localhost", Config.GTWYEXTRNL, "compileandrun", "Not good code");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(resp == null) {
            assert(false);
        } else {
            assertEquals(400, resp.getCode());
        }
    }

    @Test
    public void test4_killingFollower()
    {
        servers.get(1).shutdown();
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertNull(gateway.getPeerByID(1L));
    }

    @Test
    public void test5_killingLeader()
    {
        servers.get(7).shutdown();
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //Leader could sometimes end up as 5 if 6 is too slow to join the election and everyone already decides that 5 is the leader
        //If so, 6 will yield to the new epoch's leader
        assertTrue(gateway.getCurrentLeader().getCandidateID() == 6 || gateway.getCurrentLeader().getCandidateID() == 5);
    }

    //The unsafe cast was supplied by prof. diament, and I did not see another way to do it
    @SuppressWarnings("unchecked")
    private static void createServers()
    {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
        for (int i = 0; i < ports.length; i++)
        {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", ports[i]));
        }
        //create servers
        servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet())
        {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            ZooKeeperPeerServer server;
            if(ports[entry.getKey().intValue()] == GATEWAYPORT) {
                server = new Gateway(entry.getValue().getPort(), 0, entry.getKey(), map);
                gateway = server;
            } else {
                server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
            }
            servers.add(server);
            new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private static void stopServers()
    {
        for (ZooKeeperPeerServer server : servers)
        {
            server.shutdown();
        }
    }
    private static void suppressPrints() {
        out = new ByteArrayOutputStream();
        oldOut = System.out;
        System.setOut(new PrintStream(out));
    }
    private static void resetSystemOut() {
        System.out.flush();
        System.setOut(oldOut);
    }
}