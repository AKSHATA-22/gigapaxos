package edu.umass.cs.consistency.MonotonicReads;

import edu.umass.cs.consistency.Quorum.QuorumRequestPacket;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.testing.TESTPaxosMain;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TESTMRClient extends ReconfigurableAppClientAsync<MRRequestPacket> {
    public String[] types = new String[]{"C", "U"};
    public String[] items = new String[]{"CIRCLE01", "TRIANGLE01", "CIRCLE02", "TRIANGLE02", "CIRCLE03"};
    public String[] coords = new String[]{"1,1", "3,2", "4,4", "9,4", "8,5", "3,7", "6,3", "8,0"};
    public int[] ports = new int[]{2000, 2001, 2002};
    private HashMap<Integer, ArrayList<MRManager.Write>> requestWrites = new HashMap<>();
    private HashMap<Integer, Timestamp> requestVectorClock = new HashMap<Integer, Timestamp>();
    private boolean passed = true;
    static final Logger log = Logger.getLogger(TESTMRClient.class
            .getName());

    public TESTMRClient() throws IOException {
        super();
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
//        System.out.println("In getRequest of client");
        try {
//            System.out.println(stringified);
            return new MRRequestPacket(new JSONObject(stringified));
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(MRRequestPacket.MRPacketType.values()));
    }

    public MRRequestPacket makeWriteRequest(TESTMRClient mrc) {
        int type = (int) (Math.random() * (mrc.types.length));
        int item = (int) (Math.random() * (mrc.items.length));
        int coord = (int) (Math.random() * (mrc.coords.length));
        String command = mrc.types[type] + " " + mrc.items[item] + " " + mrc.coords[coord];
        return new MRRequestPacket((long) (Math.random() * Integer.MAX_VALUE), MRRequestPacket.MRPacketType.WRITE, MRManager.getDefaultServiceName(),
                command, mrc.requestVectorClock, mrc.requestWrites);
    }

    public MRRequestPacket makeReadRequest(TESTMRClient mrc) {
        return new MRRequestPacket((long) (Math.random() * Integer.MAX_VALUE), MRRequestPacket.MRPacketType.READ, MRManager.getDefaultServiceName(),
                "read_request", mrc.requestVectorClock, mrc.requestWrites);
    }

    public void updateWrites(MRRequestPacket response, TESTMRClient mrc) {
//        System.out.println("Response: "+response);
        mrc.requestVectorClock = response.getResponseVectorClock();
        if (response.getResponseWrites().containsKey(response.getSource())) {
            for (MRManager.Write write : response.getResponseWrites().get(response.getSource())) {
                if (!mrc.requestWrites.containsKey(response.getSource())) {
                    mrc.requestWrites.put(response.getSource(), new ArrayList<>());
                }
                mrc.requestWrites.get(response.getSource()).add(write);
            }
        }
    }
    public boolean checkRequestVectorClock(HashMap<Integer, Timestamp> receivedRVC){
        for (Integer key: receivedRVC.keySet()){
            if (receivedRVC.get(key).compareTo(requestVectorClock.get(key)) < 0){
                log.log(Level.WARNING, "Received: {0}, Given: {1}",new Object[]{receivedRVC.get(key), this.requestVectorClock.get(key)});
                return false;
            }
        }
        return true;
    }
    @Test
    public void sendReadRequestFromMultipleClientToRandomServer() throws IOException, InterruptedException{
        TESTMRClient mrClient = new TESTMRClient();
        for (int i = 0; i < 100; i++) {
            MRRequestPacket request;
            request = i % 2 == 0 ? makeWriteRequest(mrClient) : makeReadRequest(mrClient);
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
                            passed &= checkRequestVectorClock(((MRRequestPacket) response).getResponseVectorClock());
                            updateWrites(((MRRequestPacket) response), mrClient);
                            return (MRRequestPacket) response;
                        }
                    });
            Thread.sleep(100);
        }
        Assert.assertTrue(passed);
    }
    @Test
    public void sendRequestFromSingleClientToRandomServer() throws IOException, InterruptedException{
        TESTMRClient mrClient = new TESTMRClient();
        for (int i = 0; i < 100; i++) {
            MRRequestPacket request;
            request = i % 2 == 0 ? makeWriteRequest(mrClient) : makeReadRequest(mrClient);
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
                            passed &= checkRequestVectorClock(((MRRequestPacket) response).getResponseVectorClock());
                            updateWrites(((MRRequestPacket) response), mrClient);
                            return (MRRequestPacket) response;
                        }
                    });
            Thread.sleep(100);
        }
        Assert.assertTrue(passed);
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        Result result = JUnitCore.runClasses(TESTMRClient.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }

    }
}
