package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;

public class StatusReportPacket<NodeIDType> extends JSONPacket implements ReplicableRequest, ClientRequest {
    private final long requestID;
    private DynamoRequestPacket.DynamoPacket requestPacket = null;
    private int destination;
    private int source;
    private String quorumID;
    private Timestamp requestTimestamp = new Timestamp(0);
    private DynamoRequestPacket.DynamoPacketType packetType = DynamoRequestPacket.DynamoPacketType.STATUS_REPORT;
    public StatusReportPacket(long requestID, DynamoRequestPacket.DynamoPacket requestPacket, Timestamp requestTimestamp){
        super(DynamoRequestPacket.DynamoPacketType.STATUS_REPORT);
        this.requestID = requestID;
        this.requestPacket = requestPacket;
        this.requestTimestamp = requestTimestamp;
    }
    public StatusReportPacket(JSONObject json) throws JSONException {
        super(json);
        System.out.println(json);
        this.requestID = json.getLong("requestID");
        this.packetType = DynamoRequestPacket.DynamoPacketType.getDynamoPacketType(json.getInt("type"));
        JSONObject requestVectorClock = new JSONObject(json.getString("requestVectorClock"));
        HashMap<Integer, Integer> requestVCMap = new HashMap<>();
        if (requestVectorClock.length() != 0) {
            for (Iterator it = requestVectorClock.keys(); it.hasNext(); ) {
                int i = Integer.parseInt(it.next().toString());
                requestVCMap.put(i, Integer.parseInt(requestVectorClock.get(String.valueOf(i)).toString()));
            }
        }
        this.requestPacket = new DynamoRequestPacket.DynamoPacket(requestVCMap, json.getString("requestValue"));
        this.source = Integer.parseInt(json.getString("source"));
        this.destination = Integer.parseInt(json.getString("destination"));
        this.requestTimestamp = Timestamp.valueOf(json.getString("requestTimestamp"));
        this.quorumID = json.getString("quorumID");
    }

    public int getDestination() {
        return destination;
    }

    public void setDestination(int destination) {
        this.destination = destination;
    }

    public int getSource() {
        return source;
    }

    public void setSource(int source) {
        this.source = source;
    }

    @Override
    public ClientRequest getResponse() {
        return this;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return this.packetType;
    }

    @Override
    public String getServiceName() {
        return this.quorumID;
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }

    public DynamoRequestPacket.DynamoPacket getRequestPacket() {
        return requestPacket;
    }

    public void setRequestPacket(DynamoRequestPacket.DynamoPacket requestPacket) {
        this.requestPacket = requestPacket;
    }

    public Timestamp getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(Timestamp requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }

    public String getQuorumID() {
        return quorumID;
    }

    public void setQuorumID(String quorumID) {
        this.quorumID = quorumID;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("requestID", this.requestID);
        json.put("type", this.packetType.getInt());
        json.put("requestVectorClock", this.requestPacket.getVectorClock());
        json.put("requestValue", this.requestPacket.getValue());
        json.put("source", this.source);
        json.put("destination", this.destination);
        json.put("requestTimestamp", this.requestTimestamp);
        json.put("quorumID", this.quorumID);
        return json;
    }

    @Override
    public String toString() {
        try {
            return this.toJSONObjectImpl().toString();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }
}
