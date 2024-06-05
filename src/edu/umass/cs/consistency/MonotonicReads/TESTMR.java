package edu.umass.cs.consistency.MonotonicReads;

import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import org.junit.Assert;
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
    private AtomicBoolean passed = new AtomicBoolean(true);
    static final Logger log = TESTMRClient.log;
    private void threadSendingWriteRequests(TESTMRClient testmrClient){
        Runnable clientTask = () -> {
            while (keepRunning.get()) {
                try {
                    MRRequestPacket request = testmrClient.makeWriteRequest(testmrClient);
                    log.log(Level.INFO, "Sent write request from client 0");
                    testmrClient.sendRequest(request,
                            new InetSocketAddress("localhost", testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]),
                            new Callback<Request, MRRequestPacket>() {

                                long createTime = System.currentTimeMillis();

                                @Override
                                public MRRequestPacket processResponse(Request response) {
                                    assert (response instanceof MRRequestPacket) :
                                            response.getSummary();

                                    log.log(Level.INFO, "Response for request ["
                                            + request.getSummary()
                                            + " "
                                            + request.getRequestValue()
                                            + "] = "
                                            + ((MRRequestPacket) response).getResponseValue()
                                            + " received in "
                                            + (System.currentTimeMillis() - createTime)
                                            + "ms");
                                    System.out
                                            .println("Response for request ["
                                                    + request.getSummary()
                                                    + " "
                                                    + request.getRequestValue()
                                                    + "] = "
                                                    + ((MRRequestPacket) response).getResponseValue()
                                                    + " received in "
                                                    + (System.currentTimeMillis() - createTime)
                                                    + "ms");
                                    passed.set(passed.get() & testmrClient.checkRequestVectorClock(((MRRequestPacket) response).getResponseVectorClock()));
                                    testmrClient.updateWrites(((MRRequestPacket) response), testmrClient);
                                    return (MRRequestPacket) response;
                                }
                            });
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Thread clientThread = new Thread(clientTask);
        clientThread.start();
    }

    @Test
    public void sendReadRequestFromMultipleClientsToRandomServer() throws IOException {
        ArrayList<TESTMRClient> clients = new ArrayList<TESTMRClient>();
        for (int i = 0; i < noOfClients; i++) {
            clients.add(new TESTMRClient());
        }
        threadSendingWriteRequests(clients.get(0));
        for (int i = 0; i < 10; i++) {
            TESTMRClient mrClient = clients.get((int) (Math.random() * (noOfClients)));
            MRRequestPacket request;
            request = mrClient.makeReadRequest(mrClient);
            long reqInitime = System.currentTimeMillis();
//            System.out.println("Sending request vc:"+request.getRequestVectorClock());
            mrClient.sendRequest(request,
                    new InetSocketAddress("localhost", mrClient.ports[(int) (Math.random() * (mrClient.ports.length))]),
                    new Callback<Request, MRRequestPacket>() {

                        long createTime = System.currentTimeMillis();

                        @Override
                        public MRRequestPacket processResponse(Request response) {
                            assert (response instanceof MRRequestPacket) :
                                    response.getSummary();

                            log.log(Level.INFO, "Response for request ["
                                    + request.getSummary()
                                    + " "
                                    + request.getRequestValue()
                                    + "] = "
                                    + ((MRRequestPacket) response).getResponseValue()
                                    + " received in "
                                    + (System.currentTimeMillis() - createTime)
                                    + "ms");
                            System.out
                                    .println("Response for request ["
                                            + request.getSummary()
                                            + " "
                                            + request.getRequestValue()
                                            + "] = "
                                            + ((MRRequestPacket) response).getResponseValue()
                                            + " received in "
                                            + (System.currentTimeMillis() - createTime)
                                            + "ms");
                            passed.set(passed.get() & mrClient.checkRequestVectorClock(((MRRequestPacket) response).getResponseVectorClock()));
                            mrClient.updateWrites(((MRRequestPacket) response), mrClient);
                            return (MRRequestPacket) response;
                        }
                    });
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        keepRunning.set(false);
        Assert.assertTrue(passed.get());
    }
    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(TESTMRClient.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }
        Result result01 = JUnitCore.runClasses(TESTMR.class);
        for (Failure failure : result01.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }
    }
}
