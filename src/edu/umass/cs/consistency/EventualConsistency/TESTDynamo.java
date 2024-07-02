package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.ClientCentric.TESTMW;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TESTDynamo {
    public static AtomicBoolean passed = new AtomicBoolean(true);
    private static TESTDynamoClient testDynamoClient;
    @BeforeClass
    public static void initialize() throws IOException {
        testDynamoClient = new TESTDynamoClient();
    }

    @Test
    public void test01_twoPutOneGetOnDifferentServers() throws Exception{
        System.out.println("TEST 01 starting");
        testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 0), testDynamoClient.ports[0]);
        testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 0), testDynamoClient.ports[1]);
        Thread.sleep(100);
        testDynamoClient.sendAppRequest(TESTDynamoClient.makeGetRequest(testDynamoClient, 0), testDynamoClient.ports[2]);
        Thread.sleep(200);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test02_twoPutOneGetOnDifferentServersForDifferentObjects() throws Exception{
        System.out.println("TEST 02 starting");
        testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 0), testDynamoClient.ports[2]);
        testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 1), testDynamoClient.ports[0]);
        Thread.sleep(100);
        testDynamoClient.sendAppRequest(TESTDynamoClient.makeGetRequest(testDynamoClient, 0), testDynamoClient.ports[1]);
        Thread.sleep(100);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test03_getAnObjectNeverPut() throws Exception{
        System.out.println("TEST 03 starting");
        testDynamoClient.sendAppRequest(TESTDynamoClient.makeGetRequest(testDynamoClient, 2), testDynamoClient.ports[0]);
        Thread.sleep(100);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test04_sendRandomNumerousRequests() throws Exception{
        Thread.sleep(100);
        System.out.println("TEST 04 starting");
        for (int i = 0; i < 20; i++) {
            int port = (int) (Math.random() * ((testDynamoClient.ports.length - 1) + 1));
            DynamoRequestPacket dynamoRequestPacket = i % 2 == 0 ? TESTDynamoClient.makePutRequest(testDynamoClient, -1) : TESTDynamoClient.makeGetRequest(testDynamoClient, -1);
            testDynamoClient.sendAppRequest(dynamoRequestPacket, testDynamoClient.ports[port]);
            Thread.sleep(500);
        }
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test05_checkVC() throws Exception{
        Thread.sleep(100);
        System.out.println("TEST 05 starting");
        int entry = 0;
        int item = 0;
        DynamoRequestPacket dynamoRequestPacket = TESTDynamoClient.makeTestGetVCRequest(testDynamoClient, item);
        ArrayList<HashMap<Integer, Integer>> allVC = testDynamoClient.sendTESTRequest(dynamoRequestPacket, testDynamoClient.ports[entry]);
        for(HashMap<Integer, Integer> vectorClock: allVC){
            DynamoRequestPacket dynamoGetRequestPacketForEntry = TESTDynamoClient.makeTestGetRequests(testDynamoClient, item);
            dynamoGetRequestPacketForEntry.setTestRequestVectorClock(vectorClock);
            HashMap<Long, String> fromEntryServer = testDynamoClient.sendTESTGetRequest(dynamoGetRequestPacketForEntry, testDynamoClient.ports[entry]);
            System.out.println(fromEntryServer);
            for (int port = 0; port < testDynamoClient.ports.length; port++) {
                if (port != entry) {
                    DynamoRequestPacket dynamoGetRequestPacket = TESTDynamoClient.makeTestGetRequests(testDynamoClient, item);
                    dynamoGetRequestPacket.setTestRequestVectorClock(vectorClock);
                    HashMap<Long, String> notFromEntryServer = testDynamoClient.sendTESTGetRequest(dynamoGetRequestPacket, testDynamoClient.ports[port]);
                    Assert.assertTrue(fromEntryServer.keySet().containsAll(notFromEntryServer.keySet()));
                }
            }
        }
        Assert.assertTrue(true);
    }
    public static void main(String[] args) {
        Class<?> testClass = TESTMW.class;
        Method[] methods = testClass.getDeclaredMethods();

        for (Method method : methods) {
            if (method.isAnnotationPresent(Test.class)) {
                System.out.println("Test: " + method.getName() + " initialized");
            }
        }

        JUnitCore runner = new JUnitCore();
        Result r = runner.run(TESTDynamo.class);
        if(r.getFailures().isEmpty()){
            System.out.println("All test cases passed");
        }
        else {
            for (Failure failure : r.getFailures()) {
                System.out.println("Test case failed: "+failure.getDescription());
            }
        }
    }
}
