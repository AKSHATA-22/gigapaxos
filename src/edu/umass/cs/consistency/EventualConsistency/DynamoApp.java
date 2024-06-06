package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.gigapaxos.interfaces.Reconcilable;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DynamoApp implements Reconcilable {
    public String name = "DynamoReplicationApp";
    private HashMap<String, Integer> cart = new HashMap<>();
    private Logger log = Logger.getLogger(DynamoApp.class.getName());
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        System.out.println("In get request of app");
        DynamoRequestPacket dynamoRequestPacket = null;
        try {
            dynamoRequestPacket = new DynamoRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            log.log(Level.WARNING, "Unable to parse request " + stringified);
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
        if (request instanceof DynamoRequestPacket) {
            if (((DynamoRequestPacket) request).getType() == DynamoRequestPacket.DynamoPacketType.GET_FWD) {
                log.log(Level.INFO, "GET request for index: "+((DynamoRequestPacket) request).getRequestValue());
                try {
                    ((DynamoRequestPacket) request).getResponsePacket().setValue(this.cart.get(((DynamoRequestPacket) request).getRequestValue()));
                }
                catch (Exception e){

                    ((DynamoRequestPacket) request).getResponsePacket().setValue(-1);
                }
            }
            else{
                log.log(Level.INFO, "In Dynamo App for PUT request");
                try {
                    JSONObject jsonObject = new JSONObject(((DynamoRequestPacket) request).getRequestValue());
                    if (this.cart.get(jsonObject.getString("key")) != null){
                        this.cart.put(jsonObject.getString("key"), this.cart.get(jsonObject.getString("key"))+1);
                    }
                    else {
                        this.cart.put(jsonObject.getString("key"), 1);
                    }
                } catch (JSONException e) {
                    log.log(Level.WARNING, "Check the request value");
                    throw new RuntimeException(e);
                }
            }
            log.log(Level.INFO, "After execution: "+this.cart.toString());
            return true;
        }
        log.log(Level.WARNING, "Unknown request type: " + request.getRequestType());
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
    @Override
    public Request reconcile(ArrayList<Request> requests) {
        if (requests.size() == 0){
            log.log(Level.WARNING, "Reconcile method called on an empty array of requests.");
            return null;
        }
        try {
            DynamoRequestPacket dynamoRequestPacket = (DynamoRequestPacket) requests.get(0);
            for (int j = 1; j < requests.size(); j++) {
                DynamoRequestPacket currectPacket = (DynamoRequestPacket) requests.get(j);
                HashMap<Integer, Integer> vectorClock = currectPacket.getResponsePacket().getVectorClock();
                int value = currectPacket.getResponsePacket().getValue();
                Timestamp ts = currectPacket.getTimestamp();
                if (!dynamoRequestPacket.getResponsePacket().getVectorClock().isEmpty()){
                    boolean greater = true;
                    boolean smaller = true;
                    for (int key: vectorClock.keySet()) {
                        if(dynamoRequestPacket.getResponsePacket().getVectorClock().get(key) > vectorClock.get(key)){
                            greater &= true;
                            smaller &= false;
                        }
                        else if(dynamoRequestPacket.getResponsePacket().getVectorClock().get(key) < vectorClock.get(key)){
                            greater &= false;
                            smaller &= true;
                        }
                    }
                    if (smaller || greater){
                        if (smaller) {
                            dynamoRequestPacket.getResponsePacket().setVectorClock(vectorClock);
                            dynamoRequestPacket.getResponsePacket().setValue(value);
                            dynamoRequestPacket.setTimestamp(ts);
                        }
                    }
                    else {
                        if(dynamoRequestPacket.getTimestamp().compareTo(ts) < 0){
                            dynamoRequestPacket.getResponsePacket().setVectorClock(vectorClock);
                            dynamoRequestPacket.getResponsePacket().setValue(value);
                            dynamoRequestPacket.setTimestamp(ts);
                        }
                    }
                }
                else {
                    dynamoRequestPacket.setResponsePacket(new DynamoRequestPacket.DynamoPacket(vectorClock, value));
                    dynamoRequestPacket.setTimestamp(ts);
                }
            }
            return dynamoRequestPacket;
        }
        catch (Exception e){
            log.log(Level.WARNING, "Error in roconciling the requests: "+e.toString());
        }
        return null;
    }
}
