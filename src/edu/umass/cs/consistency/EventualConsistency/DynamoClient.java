package edu.umass.cs.consistency.EventualConsistency;
import edu.umass.cs.consistency.Quorum.QuorumRequestPacket;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DynamoClient extends ReconfigurableAppClientAsync<DynamoRequestPacket> {
    private final String[] items = new String[]{"table", "chair", "pen"};
    private final int[] ports = new int[]{2000,2001,2002};
    public DynamoClient() throws IOException {
        super();
    }
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
//        System.out.println("In getRequest of client");
        try {
//            System.out.println(stringified);
            return new DynamoRequestPacket(new JSONObject(stringified));
        }
        catch (Exception e){
            System.out.println("Exception: "+e);
        }
        return null;
    }
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>(Arrays.asList(DynamoRequestPacket.DynamoPacketType.values()));
    }
    public static DynamoRequestPacket makePutRequest(DynamoClient dc){
        int randomNum = (int)(Math.random() * ((dc.items.length-1) + 1));
        JSONObject jsonObject = new JSONObject();
        String putString = dc.items[randomNum];
        try {
            jsonObject.put("key", putString);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return new DynamoRequestPacket((long)(Math.random()*Integer.MAX_VALUE),
                jsonObject.toString(), DynamoRequestPacket.DynamoPacketType.PUT, DynamoManager.getDefaultServiceName());
    }
    private static DynamoRequestPacket makeGetRequest(DynamoClient dc){
        int randomNum = (int)(Math.random() * ((dc.items.length-1) + 1));
        String getString = dc.items[randomNum];
        return new DynamoRequestPacket((long)(Math.random()*Integer.MAX_VALUE),
                getString, DynamoRequestPacket.DynamoPacketType.GET, DynamoManager.getDefaultServiceName());
    }
    public static void main(String[] args) throws IOException, InterruptedException{
        DynamoClient dynamoClient = new DynamoClient();

        for (int i = 0; i < 10; i++) {
            DynamoRequestPacket request;
            request = i%2==0 ? makePutRequest(dynamoClient) : makeGetRequest(dynamoClient);
            long reqInitime = System.currentTimeMillis();
//            System.out.println(request);
            dynamoClient.sendRequest(request ,
                    new InetSocketAddress("localhost", dynamoClient.ports[(int)(Math.random() * ((dynamoClient.ports.length-1) + 1))]),
                    new Callback<Request, DynamoRequestPacket>() {

                        long createTime = System.currentTimeMillis();
                        @Override
                        public DynamoRequestPacket processResponse(Request response) {
                            assert(response instanceof QuorumRequestPacket) :
                                    response.getSummary();
                            System.out
                                    .println("Response for request ["
                                            + request.getSummary()
                                            + " "
                                            + request.getRequestValue()
                                            + "] = "
                                            + ((DynamoRequestPacket)response).getResponsePacket()
                                            + " "
                                            + ((DynamoRequestPacket)response).getTimestamp()
                                            + " sent at "
                                            + (createTime)
                                            + "ms");
                            return (DynamoRequestPacket) response;
                        }
                    });
            Thread.sleep(1000);
        }

    }
}
