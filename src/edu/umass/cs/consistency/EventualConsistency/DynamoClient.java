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
    public DynamoClient() throws IOException {
        super();
    }
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        System.out.println("In getRequest of client");
        try {
            return new DynamoRequestPacket(new JSONObject(stringified));
        }
        catch (Exception e){
            System.out.println("Exception: "+e);
        }
        return null;
    }
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(DynamoRequestPacket.DynamoPacketType.values()));
    }
    public static void main(String[] args) throws IOException, InterruptedException{
        DynamoClient dynamoClient = new DynamoClient();
        DynamoRequestPacket request;
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("key", "chair");
            jsonObject.put("value", "2");
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        request = new DynamoRequestPacket((long)(Math.random()*Integer.MAX_VALUE),
                "", DynamoRequestPacket.DynamoPacketType.PUT, DynamoManager.getDefaultServiceName());
//        request = new DynamoRequestPacket((long)(Math.random()*Integer.MAX_VALUE),
//                "Chair", DynamoRequestPacket.DynamoPacketType.GET, DynamoManager.getDefaultServiceName());
        long reqInitime = System.currentTimeMillis();
        System.out.println(request);
        dynamoClient.sendRequest(request ,
                new InetSocketAddress("localhost", 2000),
                new Callback<Request, DynamoRequestPacket>() {

                    long createTime = System.currentTimeMillis();
                    @Override
                    public DynamoRequestPacket processResponse(Request response) {
                        assert(response instanceof QuorumRequestPacket) :
                                response.getSummary();
                        System.out
                                .println("Response for request ["
                                        + request.getSummary()
                                        + "] = "
                                        + ((DynamoRequestPacket)response).getResponseArrayList()
                                        + " received in "
                                        + (System.currentTimeMillis() - createTime)
                                        + "ms");
                        return (DynamoRequestPacket) response;
                    }
                });
    }
}
