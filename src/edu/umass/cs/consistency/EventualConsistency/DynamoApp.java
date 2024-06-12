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
        try {
            return new DynamoRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            try {
                return new StatusReportPacket(new JSONObject(stringified));
            } catch (JSONException e) {
                log.log(Level.WARNING, "Unable to parse request " + stringified+" as a valid Dynamo request");
                throw new RequestParseException(e);
            }
        }
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
                if(!((DynamoRequestPacket) request).getRequestValue().isEmpty()) {
                    try {
                        ((DynamoRequestPacket) request).getResponsePacket().setValue(String.valueOf(this.cart.get(((DynamoRequestPacket) request).getRequestValue())));
                    } catch (Exception e) {
                        ((DynamoRequestPacket) request).getResponsePacket().setValue("-1");
                    }
                }
                else {
                    ((DynamoRequestPacket) request).getResponsePacket().setValue(this.cart.toString());
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
        if (request instanceof StatusReportPacket) {
            log.log(Level.INFO, "Status Report received");
            return true;
        }
        return this.execute(request);
    }

    @Override
    public String checkpoint(String name) {
        System.out.println("In checkpoint");
        return this.cart.toString();
    }
    @Override
    public String stateForReconcile(){
        StringBuilder stringBuilder = new StringBuilder();
        for (String key: this.cart.keySet()){
            stringBuilder.append(key).append("=").append(this.cart.get(key)).append(",");
        }
        return stringBuilder.toString();
    }
    public void unstringifyState(String state){
        if(!state.equals("{}")){
            for (String keyValuePair : state.split(",")) {
                String[] keyValue = keyValuePair.split("=");
                this.cart.put(keyValue[0], Integer.parseInt(keyValue[1]));
            }
        }
    }
    @Override
    public boolean restore(String name, String state) {
        System.out.println("In restore");
        this.cart = new HashMap<String, Integer>();
        if(state != null){
            try{
                unstringifyState(state);
            }
            catch (Exception e){
                System.out.println("Exception encountered: "+e);
                e.printStackTrace();
            }
        }
        return true;
    }
    @Override
    public Request reconcile(ArrayList<Request> requests) {
        if (requests.isEmpty()){
            log.log(Level.WARNING, "Reconcile method called on an empty array of requests.");
            return null;
        }
        if (requests.get(0) instanceof DynamoRequestPacket) {
            try {
                DynamoRequestPacket dynamoRequestPacket = (DynamoRequestPacket) requests.get(0);
                for (int j = 1; j < requests.size(); j++) {
                    DynamoRequestPacket currentPacket = (DynamoRequestPacket) requests.get(j);
                    HashMap<Integer, Integer> vectorClock = currentPacket.getResponsePacket().getVectorClock();
                    String value = currentPacket.getResponsePacket().getValue();
                    Timestamp ts = currentPacket.getTimestamp();
                    if (!dynamoRequestPacket.getResponsePacket().getVectorClock().isEmpty()) {
                        boolean greater = true;
                        boolean smaller = true;
                        for (int key : vectorClock.keySet()) {
                            if (dynamoRequestPacket.getResponsePacket().getVectorClock().get(key) > vectorClock.get(key)) {
                                greater &= true;
                                smaller &= false;
                            } else if (dynamoRequestPacket.getResponsePacket().getVectorClock().get(key) < vectorClock.get(key)) {
                                greater &= false;
                                smaller &= true;
                            }
                        }
                        if (smaller || greater) {
                            if (smaller) {
                                dynamoRequestPacket.getResponsePacket().setVectorClock(vectorClock);
                                dynamoRequestPacket.getResponsePacket().setValue(value);
                                dynamoRequestPacket.setTimestamp(ts);
                            }
                        } else {
                            if (dynamoRequestPacket.getTimestamp().compareTo(ts) < 0) {
                                dynamoRequestPacket.getResponsePacket().setVectorClock(vectorClock);
                                dynamoRequestPacket.getResponsePacket().setValue(value);
                                dynamoRequestPacket.setTimestamp(ts);
                            }
                        }
                    } else {
                        dynamoRequestPacket.setResponsePacket(new DynamoRequestPacket.DynamoPacket(vectorClock, value));
                        dynamoRequestPacket.setTimestamp(ts);
                    }
                }
                return dynamoRequestPacket;
            } catch (Exception e) {
                log.log(Level.WARNING, "Error in reconciling the requests: " + e.toString());
            }
        } else if (requests.get(0) instanceof StatusReportPacket) {
            try {
                StatusReportPacket statusReportPacket = (StatusReportPacket) requests.get(0);
                for (int j = 1; j < requests.size(); j++) {
                    StatusReportPacket currentPacket = (StatusReportPacket) requests.get(j);
                    HashMap<Integer, Integer> vectorClock = currentPacket.getRequestPacket().getVectorClock();
                    String value = currentPacket.getRequestPacket().getValue();
                    Timestamp ts = currentPacket.getRequestTimestamp();
                    boolean greater = true;
                    boolean smaller = true;
                    System.out.println(vectorClock);
                    System.out.println(statusReportPacket.getRequestPacket().getVectorClock());
                    for (int key : vectorClock.keySet()) {
                        if (statusReportPacket.getRequestPacket().getVectorClock().get(key) > vectorClock.get(key)) {
                            greater &= true;
                            smaller &= false;
                        } else if (statusReportPacket.getRequestPacket().getVectorClock().get(key) < vectorClock.get(key)) {
                            greater &= false;
                            smaller &= true;
                        }
                    }
                    if (smaller || greater) {
                        if (smaller & !greater) {
                            statusReportPacket.getRequestPacket().setVectorClock(vectorClock);
                            statusReportPacket.getRequestPacket().setValue(value);
                            statusReportPacket.setRequestTimestamp(ts);
                            statusReportPacket.setDestination(currentPacket.getSource());
                        }
                    } else {
                        if (statusReportPacket.getRequestTimestamp().compareTo(ts) < 0) {
                            statusReportPacket.getRequestPacket().setVectorClock(vectorClock);
                            statusReportPacket.getRequestPacket().setValue(value);
                            statusReportPacket.setRequestTimestamp(ts);
                            statusReportPacket.setDestination(currentPacket.getSource());
                        }
                    }
                }
                if(statusReportPacket.getSource() != statusReportPacket.getDestination()){
                    unstringifyState(statusReportPacket.getRequestPacket().getValue());
                }
                return statusReportPacket;
            } catch (Exception e) {
                log.log(Level.WARNING, "Error in reconciling the requests: " + e.toString());
            }
        }
        return null;
    }
}
