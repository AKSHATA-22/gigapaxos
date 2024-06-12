package edu.umass.cs.consistency.MonotonicReads;

import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TESTMR {
    private final int noOfClients = 5;
    private AtomicBoolean keepRunning = new AtomicBoolean(true);
    public static AtomicBoolean passed = new AtomicBoolean(true);
    static final Logger log = TESTMRClient.log;
    private void threadSendingWriteRequests(TESTMRClient testmrClient){
        Runnable clientTask = () -> {
            while (keepRunning.get()) {
                try {
                    MRRequestPacket request = testmrClient.makeWriteRequest(testmrClient);
                    log.log(Level.INFO, "Sent write request from client 0");
                    testmrClient.sendAppRequest(testmrClient, request, testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Thread clientThread = new Thread(clientTask);
        clientThread.start();
    }
    @Test
    public void test01_sendRequestToOneExtraServer() throws IOException{
        TESTMRClient testmrClient = new TESTMRClient();
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient), 2000);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient), 2000);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeReadRequest(testmrClient), 2001);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test02_sendRequestToRandomServers() throws IOException{
        passed.set(true);
        TESTMRClient testmrClient = new TESTMRClient();
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient), testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient), testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeReadRequest(testmrClient), testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test03_sendRandomRequestsToRandomServers() throws IOException{
        passed.set(true);
        TESTMRClient testmrClient = new TESTMRClient();
        for (int i = 0; i < 100; i++) {
            MRRequestPacket request = i % 2 == 0 ? testmrClient.makeWriteRequest(testmrClient) : testmrClient.makeReadRequest(testmrClient);
            testmrClient.sendAppRequest(testmrClient, request, testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        }
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test04_sendReadRequestFromTwoClientsToRandomServer() throws IOException {
        passed.set(true);
        ArrayList<TESTMRClient> clients = new ArrayList<TESTMRClient>();
        for (int i = 0; i < 2; i++) {
            clients.add(new TESTMRClient());
        }
        threadSendingWriteRequests(clients.get(0));
        for (int i = 0; i < 10; i++) {
            TESTMRClient mrClient = clients.get((int) (Math.random() * (2)));
            MRRequestPacket request = mrClient.makeReadRequest(mrClient);
            mrClient.sendAppRequest(mrClient, request, mrClient.ports[(int) (Math.random() * (mrClient.ports.length))]);
        }
        keepRunning.set(false);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test05_sendReadRequestFromMultipleClientsToRandomServer() throws IOException {
        passed.set(true);
        keepRunning.set(true);
        ArrayList<TESTMRClient> clients = new ArrayList<TESTMRClient>();
        for (int i = 0; i < noOfClients; i++) {
            clients.add(new TESTMRClient());
        }
        threadSendingWriteRequests(clients.get(0));
        for (int i = 0; i < 10; i++) {
            TESTMRClient mrClient = clients.get((int) (Math.random() * (noOfClients)));
            MRRequestPacket request = mrClient.makeReadRequest(mrClient);
            mrClient.sendAppRequest(mrClient, request, mrClient.ports[(int) (Math.random() * (mrClient.ports.length))]);
        }
        keepRunning.set(false);
        Assert.assertTrue(passed.get());
    }
    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(TESTMR.class);
        System.out.println(result.getFailures());
        for (Failure failure : result.getFailures()) {
            System.out.println("Test case failed: "+failure.getDescription());
        }
    }
}
