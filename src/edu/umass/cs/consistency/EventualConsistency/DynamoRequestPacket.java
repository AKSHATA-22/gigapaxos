package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;

public class DynamoRequestPacket extends JSONPacket implements ReplicableRequest, ClientRequest {
    private final long requestID;
//    Maps the versionVector hashmap to the final response string
    private String requestValue = null;
    private HashMap<Integer, Integer> requestVectorClock = new HashMap<Integer, Integer>();
    private ArrayList<DynamoPacket> response = new ArrayList<>();
    private Integer version = -1;
    private DynamoPacket responsePacket = null;
    private int destination = -1;
    private int source = -1;
    private InetSocketAddress clientSocketAddress = null;
    private DynamoPacketType packetType;
    private String quorumID = null;
    private Timestamp timestamp = new Timestamp(0);
    static class DynamoPacket{
        HashMap<Integer, Integer> vectorClock = new HashMap<>();
        String value;
        DynamoPacket(HashMap<Integer, Integer> vectorClock, String value){
            this.vectorClock = vectorClock;
            this.value = value;
        }

        public HashMap<Integer, Integer> getVectorClock() {
            return vectorClock;
        }
        public void addEntryInVectorClock(int id, int version){
            this.vectorClock.put(id, version);
        }
        public int getVersion(int id){
            return this.getVectorClock().get(id);
        }
        public void setVectorClock(HashMap<Integer, Integer> vectorClock) {
            this.vectorClock = vectorClock;
        }
        public String getValue() {
            return value;
        }
        public void setValue(String value) {
            this.value = value;
        }
        @Override
        public String toString(){
            JSONObject json = new JSONObject();
            try {
                json.put("vectorClock", this.vectorClock);
                json.put("value", this.value);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
            return json.toString();
        }
    }
    public ArrayList<String> responseToString(){
        ArrayList<String> responseStr = new ArrayList<>();
        for (int i = 0; i < this.response.size(); i++) {
            responseStr.add(this.response.get(i).toString());
        }
        return responseStr;
    }
    public DynamoPacket strToDynamoPck(String str){
        try {
            JSONObject json = new JSONObject(str);
            JSONObject vectorClockStr = new JSONObject(json.getString("vectorClock"));
            HashMap<Integer, Integer> vectorClock = new HashMap<>();
            if (vectorClockStr.length() != 0) {
                for (Iterator it = vectorClockStr.keys(); it.hasNext(); ) {
                    int i = Integer.parseInt(it.next().toString());
                    vectorClock.put(i, Integer.parseInt(vectorClockStr.get(String.valueOf(i)).toString()));
                }
            }
            System.out.println(vectorClock);
            return new DynamoPacket(vectorClock, json.getString("value"));
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    public DynamoPacket getResponsePacket(){
        return this.responsePacket;
    }
    public ArrayList<DynamoPacket> getResponseArrayList(){
        return this.response;
    }
    public void addToResponseArrayList(DynamoPacket packet){
        this.response.add(packet);
    }
    public enum DynamoPacketType implements IntegerPacketType {
        PUT("PUT", 1301),
        PUT_FWD("PUT_FWD", 1302),
        GET("GET", 1303),
        GET_FWD("GET_FWD", 1304),
        PUT_ACK("PUT_ACK", 1305),
        GET_ACK("GET_ACK", 1306),
        RESPONSE("RESPONSE", 1307),
        STATUS_REPORT("STATUS_REPORT", 1308)
        ;
        String label;
        int number;
        private static HashMap<String, DynamoPacketType> labels = new HashMap<String, DynamoPacketType>();
        private static HashMap<Integer, DynamoPacketType> numbers = new HashMap<Integer, DynamoPacketType>();
        DynamoPacketType(String s, int t) {
            this.label = s;
            this.number = t;
        }
        static {
            for (DynamoPacketType type: DynamoPacketType.values()) {
                if (!DynamoPacketType.labels.containsKey(type.label)
                        && !DynamoPacketType.numbers.containsKey(type.number)) {
                    DynamoPacketType.labels.put(type.label, type);
                    DynamoPacketType.numbers.put(type.number, type);
                } else {
                    assert(false): "Duplicate or inconsistent enum type for ChainPacketType";
                }
            }
        }
        @Override
        public int getInt() {
            return this.number;
        }
        public static DynamoPacketType getDynamoPacketType(int type){
            return DynamoPacketType.numbers.get(type);
        }

    }
    public DynamoRequestPacket(long reqID, DynamoPacketType reqType,
                               DynamoRequestPacket req){
        super(reqType);

        this.packetType = reqType;
        this.requestID = reqID;
        if(req == null)
            return;
        this.responsePacket = req.responsePacket;
        this.requestValue = req.requestValue;
        this.quorumID = req.quorumID;
        this.destination = req.destination;
        this.source = req.source;
        this.clientSocketAddress  = req.clientSocketAddress;
    }
    public DynamoRequestPacket(JSONObject json) throws JSONException{
        super(json);
//        System.out.println("In quorum request packet constructor============================");

        this.packetType = DynamoPacketType.getDynamoPacketType(json.getInt("type"));
        this.requestID = json.getLong("requestID");
        this.requestValue = json.getString("requestValue");
        this.responsePacket = new DynamoPacket(new HashMap<>(), json.getString("dynamoPacketValue"));
        JSONObject vectorClock = new JSONObject(json.getString("dynamoPacketVectorClock"));
        if (vectorClock.length() != 0) {
            for (Iterator it = vectorClock.keys(); it.hasNext(); ) {
                int i = Integer.parseInt(it.next().toString());
                this.responsePacket.addEntryInVectorClock(i, Integer.parseInt(vectorClock.get(String.valueOf(i)).toString()));
            }
        }
        JSONObject requestVectorClock = new JSONObject(json.getString("requestVectorClock"));
        if (requestVectorClock.length() != 0) {
            for (Iterator it = requestVectorClock.keys(); it.hasNext(); ) {
                int i = Integer.parseInt(it.next().toString());
                this.requestVectorClock.put(i, Integer.parseInt(requestVectorClock.get(String.valueOf(i)).toString()));
            }
        }
        this.source = json.getInt("source");
        this.destination = json.getInt("destination");
        this.quorumID = json.getString("quorumID");
        JSONArray jarray = json.getJSONArray("response");
        for (int i = 0; i < jarray.length() ;i++){
            this.response.add(this.strToDynamoPck(jarray.get(i).toString()));
        }
        this.timestamp = Timestamp.valueOf(json.getString("ts"));
    }

    public DynamoRequestPacket(long reqID, String value,
                               DynamoPacketType reqType, String quorumID){
        super(reqType);
        this.packetType = reqType;
        this.requestID = reqID;
        this.requestValue = value;
        this.quorumID = quorumID;
    }
    public void setQuorumID(String quorumID){
        this.quorumID = quorumID;
    }
    public void setVersion(Integer version){
        this.version = version;
    }
    public void setPacketType(DynamoPacketType packetType){
        this.packetType = packetType;
    }
    public void setDestination(int destination) {
        this.destination = destination;
    }
    public void setSource(int source) {
        this.source = source;
    }
    public int getDestination() {
        return destination;
    }
    public int getSource() {
        return source;
    }
    public String getRequestValue() {
        return requestValue;
    }
    public void setRequestValue(String requestValue) {
        this.requestValue = requestValue;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public HashMap<Integer, Integer> getRequestVectorClock() {
        return requestVectorClock;
    }
    public void setRequestVectorClock(HashMap<Integer, Integer> requestVectorClock) {
        this.requestVectorClock = requestVectorClock;
    }

    public void setResponsePacket(DynamoPacket responsePacket) {
        this.responsePacket = responsePacket;
    }

    @Override
    public ClientRequest getResponse() {
//        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! getResponse is called!!!!!!!!!!!!");

        DynamoRequestPacket reply = new DynamoRequestPacket(this.requestID,
                DynamoPacketType.RESPONSE, this);
//        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Response value is "+response+"!!!!!!!!!!!!");
        reply.response = this.response;
        reply.responsePacket  = this.responsePacket;
        return reply;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return this.getType();
    }
    public DynamoPacketType getType() {
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

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject json = new JSONObject();
//        convert this in enums
        json.put("quorumID", this.quorumID);
        json.put("type", this.packetType.getInt());
        json.put("requestValue", this.requestValue);
        json.put("packetType", this.packetType);
        json.put("requestVectorClock", this.requestVectorClock);
        json.put("requestID", this.requestID);
        if(this.responsePacket != null) {
            json.put("dynamoPacketVectorClock", this.responsePacket.getVectorClock());
            json.put("dynamoPacketValue", this.responsePacket.getValue());
        }
        else {
            json.put("dynamoPacketVectorClock", new JSONObject());
            json.put("dynamoPacketValue", -1);
        }
        json.put("clientSocketAddress", this.clientSocketAddress);
        json.put("destination", this.destination);
        json.put("source", this.source);
        json.put("response", this.responseToString());
        json.put("ts", this.timestamp);
        return json;
    }

    @Override
    public boolean needsCoordination() {
        return true;
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

}
