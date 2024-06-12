package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.MonotonicReads.MRRequestPacket;
import edu.umass.cs.consistency.MonotonicReads.TESTMR;
import edu.umass.cs.consistency.MonotonicReads.TESTMRClient;
import edu.umass.cs.consistency.Quorum.QuorumRequestPacket;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TESTDynamoClient extends ReconfigurableAppClientAsync<DynamoRequestPacket> {
    private final String[] items = new String[]{"table", "chair", "pen"};
    private final int[] ports = new int[]{2000, 2001, 2002};
    private final AtomicInteger responseCounter = new AtomicInteger(0);
    private AtomicBoolean passed = new AtomicBoolean(true);
    private String stateReceived = null;
    static final Logger log = Logger.getLogger(TESTDynamoClient.class.getName());
    public TESTDynamoClient() throws IOException {
        super();
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        try {
            return new DynamoRequestPacket(new JSONObject(stringified));
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>(Arrays.asList(DynamoRequestPacket.DynamoPacketType.values()));
    }

    public static DynamoRequestPacket makePutRequest(TESTDynamoClient dc) {
        int randomNum = (int) (Math.random() * ((dc.items.length - 1) + 1));
        JSONObject jsonObject = new JSONObject();
        String putString = dc.items[randomNum];
        try {
            jsonObject.put("key", putString);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return new DynamoRequestPacket((long) (Math.random() * Integer.MAX_VALUE),
                jsonObject.toString(), DynamoRequestPacket.DynamoPacketType.PUT, DynamoManager.getDefaultServiceName());
    }

    private static DynamoRequestPacket makeGetRequest(TESTDynamoClient dc) {
        int randomNum = (int) (Math.random() * ((dc.items.length - 1) + 1));
        String getString = dc.items[randomNum];
        return new DynamoRequestPacket((long) (Math.random() * Integer.MAX_VALUE),
                getString, DynamoRequestPacket.DynamoPacketType.GET, DynamoManager.getDefaultServiceName());
    }
    private static DynamoRequestPacket makeGetStateRequest(TESTDynamoClient dc) {
        return new DynamoRequestPacket((long) (Math.random() * Integer.MAX_VALUE),
                "", DynamoRequestPacket.DynamoPacketType.GET, DynamoManager.getDefaultServiceName());
    }
    public boolean allStateEqual() throws IOException, InterruptedException {
        TESTDynamoClient testDynamoClient = new TESTDynamoClient();
        DynamoRequestPacket request = makeGetStateRequest(testDynamoClient);
        log.log(Level.INFO, "Sent write request from client 0");
        for (int port: testDynamoClient.ports) {
            testDynamoClient.sendRequest(request,
                    new InetSocketAddress("localhost", port),
                    new Callback<Request, DynamoRequestPacket>() {

                        long createTime = System.currentTimeMillis();

                        @Override
                        public DynamoRequestPacket processResponse(Request response) {
                            assert (response instanceof DynamoRequestPacket) :
                                    response.getSummary();

                            log.log(Level.INFO, "Response for request ["
                                    + request.getSummary()
                                    + " "
                                    + request.getRequestValue()
                                    + "] = "
                                    + ((DynamoRequestPacket) response).getResponsePacket()
                                    + " received in "
                                    + (System.currentTimeMillis() - createTime)
                                    + "ms");
                            System.out
                                    .println("Response for request ["
                                            + request.getSummary()
                                            + " "
                                            + request.getRequestValue()
                                            + "] = "
                                            + ((DynamoRequestPacket) response).getResponsePacket()
                                            + " received in "
                                            + (System.currentTimeMillis() - createTime)
                                            + "ms");
                            if(!stateReceived.isEmpty()){
                                if(!stateReceived.equals(((DynamoRequestPacket) response).getResponsePacket().getValue())){
                                    passed.set(false);
                                }
                            }
                            else {
                                stateReceived = ((DynamoRequestPacket) response).getResponsePacket().getValue();
                            }
                            responseCounter.set(responseCounter.get()+1);
                            return (DynamoRequestPacket) response;
                        }
                    });
        }
        while (responseCounter.get() != ports.length){
            System.out.println(responseCounter.get()+" "+ports.length);
            Thread.sleep(100);
        }
        return passed.get();
    }
    @Test
    public void sendingPutRequests() throws IOException, InterruptedException {
        TESTDynamoClient testDynamoClient = new TESTDynamoClient();
        for (int i = 0; i < 50; i++) {
            try {
                DynamoRequestPacket request = makePutRequest(testDynamoClient);
                log.log(Level.INFO, "Sent write request from client 0");
                testDynamoClient.sendRequest(request,
                        new InetSocketAddress("localhost", testDynamoClient.ports[(int) (Math.random() * (testDynamoClient.ports.length))]),
                        new Callback<Request, DynamoRequestPacket>() {

                            long createTime = System.currentTimeMillis();

                            @Override
                            public DynamoRequestPacket processResponse(Request response) {
                                assert (response instanceof DynamoRequestPacket) :
                                        response.getSummary();

                                log.log(Level.INFO, "Response for request ["
                                        + request.getSummary()
                                        + " "
                                        + request.getRequestValue()
                                        + "] = "
                                        + ((DynamoRequestPacket) response).getResponsePacket()
                                        + " received in "
                                        + (System.currentTimeMillis() - createTime)
                                        + "ms");
                                System.out
                                        .println("Response for request ["
                                                + request.getSummary()
                                                + " "
                                                + request.getRequestValue()
                                                + "] = "
                                                + ((DynamoRequestPacket) response).getResponsePacket()
                                                + " received in "
                                                + (System.currentTimeMillis() - createTime)
                                                + "ms");
//                                    passed.set(passed.get() & testDynamoClient.checkRequestVectorClock(((MRRequestPacket) response).getResponseVectorClock()));
//                                    testDynamoClient.updateWrites(((MRRequestPacket) response), testDynamoClient);
                                return (DynamoRequestPacket) response;
                            }
                        });
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        Thread.sleep(1000);
        Assert.assertTrue(allStateEqual());
    }


    public static void main(String[] args) throws IOException {
        Result result = JUnitCore.runClasses(TESTDynamoClient.class);
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
