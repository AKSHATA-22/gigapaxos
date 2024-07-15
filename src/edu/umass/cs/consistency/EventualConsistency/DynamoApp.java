package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.EventualConsistency.Domain.DAG;
import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;
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
        try {
            return new DynamoRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            throw new RequestParseException(je);
        }
    }
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(DynamoRequestPacket.DynamoPacketType.values()));
    }
    private Set<IntegerPacketType> getGETRequestTypes() {
        ArrayList<DynamoRequestPacket.DynamoPacketType> GETTypes = new ArrayList<>();
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.GET);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.GET_FWD);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.GET_ACK);
        return new HashSet<IntegerPacketType>(GETTypes);
    }
    private Set<IntegerPacketType> getPUTRequestTypes() {
        ArrayList<DynamoRequestPacket.DynamoPacketType> GETTypes = new ArrayList<>();
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.PUT);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.PUT_FWD);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.PUT_ACK);
        return new HashSet<IntegerPacketType>(GETTypes);
    }
    @Override
    public boolean execute(Request request) {
        if (request instanceof DynamoRequestPacket) {
            if (getGETRequestTypes().contains(((DynamoRequestPacket) request).getType())) {
                log.log(Level.INFO, "GET request for index: "+((DynamoRequestPacket) request).getRequestValue());
                if(!((DynamoRequestPacket) request).getRequestValue().isEmpty()) {
                    try {
                        JSONObject jsonObject = new JSONObject(((DynamoRequestPacket) request).getRequestValue());
                        ((DynamoRequestPacket) request).getResponsePacket().setValue(String.valueOf(this.cart.get(jsonObject.getString("key"))));
                    } catch (Exception e) {
                        ((DynamoRequestPacket) request).getResponsePacket().setValue("0");
                    }
                }
                else {
                    ((DynamoRequestPacket) request).getResponsePacket().setValue(this.cart.toString());
                }
            }
            else if(getPUTRequestTypes().contains(((DynamoRequestPacket) request).getType())){
                log.log(Level.INFO, "In Dynamo App for PUT request");
                try {
                    System.out.println("-----------------App PUT"+((DynamoRequestPacket) request).getRequestValue()+"--"+((DynamoRequestPacket) request).getRequestID()+"----------------------------");
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
    public GraphNode reconcile(ArrayList<GraphNode> requests) {
        if (requests.isEmpty()){
            log.log(Level.WARNING, "Reconcile method called on an empty array of requests.");
            return null;
        }
        try {
            return DAG.getDominantVC(requests);
        } catch (Exception e) {
            log.log(Level.WARNING, "Error in reconciling the requests: " + e.toString());
        }
        return null;
    }
}
