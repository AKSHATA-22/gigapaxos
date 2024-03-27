package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DynamoApp implements Replicable {
    public String name = "DynamoReplicationApp";
    private HashMap<String, Integer> cart = new HashMap<>();
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        System.out.println("In get request of app");
        DynamoRequestPacket dynamoRequestPacket = null;
        try {
            dynamoRequestPacket = new DynamoRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            ReconfigurationConfig.getLogger().info(
                    "Unable to parse request " + stringified);
            throw new RequestParseException(je);
        }
        return dynamoRequestPacket;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(DynamoRequestPacket.DynamoPacketType.values()));
    }

    @Override
    public boolean execute(Request request) {
        System.out.println("In execute request of Dynamo Replication");
        this.cart.put("Chair", 2);
        if (request instanceof DynamoRequestPacket) {
            if (((DynamoRequestPacket) request).getType() == DynamoRequestPacket.DynamoPacketType.GET_FWD) {
                System.out.println("GET request for index: "+((DynamoRequestPacket) request).getRequestValue());
                try {
                    ((DynamoRequestPacket) request).getResponsePacket().setValue(this.cart.get(((DynamoRequestPacket) request).getRequestValue()));
                }
                catch (Exception e){
                    ((DynamoRequestPacket) request).getResponsePacket().setValue(-1);
                }
            }
            else{
                System.out.println("In Dynamo App for PUT request");
                try {
                    JSONObject jsonObject = new JSONObject(((DynamoRequestPacket) request).getRequestValue());
                    this.cart.put(jsonObject.getString("key"), jsonObject.getInt("value"));
                } catch (JSONException e) {
                    System.out.println("Check the request value");
                    throw new RuntimeException(e);
                }
            }

            System.out.println("After execution: "+this.cart.toString());
            return true;
        }
        else System.err.println("Unknown request type: " + request.getRequestType());
        return false;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        System.out.println("In other execute");
        return this.execute(request);
    }

    @Override
    public String checkpoint(String name) {
        System.out.println("In checkpoint");
        return this.cart.toString();
    }

    @Override
    public boolean restore(String name, String state) {
        System.out.println("In restore");
        this.cart = new HashMap<String, Integer>();
        if(state != null){
            try{
                for (String keyValuePair : state.split(", ")) {
                    String[] keyValue = keyValuePair.split("=");
                    this.cart.put(keyValue[0], Integer.parseInt(keyValue[1]));
                }
            }
            catch (Exception e){
                System.out.println("Exception");
                e.printStackTrace();
            }
        }
        return true;
    }
}
