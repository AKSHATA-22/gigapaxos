package edu.umass.cs.consistency.MonotonicReads;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
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

public class MRRequestPacket extends JSONPacket implements ReplicableRequest, ClientRequest {
    public final long requestID;
    //    Maps the versionVector hashmap to the final response string
    private String requestValue = null;
    private String serviceName = null;
    private HashMap<Integer, Timestamp> requestVectorClock = new HashMap<Integer, Timestamp>();
    private HashMap<Integer, Timestamp> responseVectorClock = new HashMap<Integer, Timestamp>();
    private HashMap<Integer, ArrayList<MRManager.Write>> requestWrites = new HashMap<>();
    private HashMap<Integer, ArrayList<MRManager.Write>> responseWrites = new HashMap<>();
    private String responseValue = "";
    private Timestamp writesTo = new Timestamp(0);
    private Timestamp writesFrom = new Timestamp(0);
    private int destination = -1;
    private int source = -1;
    private InetSocketAddress clientSocketAddress = null;
    private MRPacketType packetType;

    public MRRequestPacket(long reqID, MRPacketType reqType, String serviceName, String value, HashMap<Integer, Timestamp> requestVectorClock, HashMap<Integer, ArrayList<MRManager.Write>> requestWrites){
        super(reqType);
        this.packetType = reqType;
        this.requestID = reqID;
        this.requestValue = value;
        this.requestWrites = requestWrites;
        this.requestVectorClock = requestVectorClock;
        this.serviceName = serviceName;
    }
    public MRRequestPacket(long reqID, MRPacketType reqType,
                               MRRequestPacket req){
        super(reqType);

        this.packetType = reqType;
        this.requestID = reqID;
        if(req == null)
            return;
        this.requestValue = req.requestValue;
        this.serviceName = req.serviceName;
        this.clientSocketAddress  = req.clientSocketAddress;
        this.responseValue = req.responseValue;
        this.requestVectorClock = req.requestVectorClock;
        this.responseVectorClock = req.responseVectorClock;
        this.requestWrites = req.requestWrites;
        this.responseWrites = req.responseWrites;
        this.writesTo = req.writesTo;
        this.writesFrom = req.writesFrom;
        this.destination = req.destination;
        this.source = req.source;
    }
    public MRRequestPacket(JSONObject jsonObject) throws JSONException{
        super(jsonObject);
        this.requestID = jsonObject.getLong("requestID");
        this.setPacketType(MRPacketType.getMRPacketType(jsonObject.getInt("type")));
        if (jsonObject.has("serviceName")){
            this.setServiceName(jsonObject.getString("serviceName"));
            this.setRequestValue(jsonObject.getString("requestValue"));
            this.setResponseValue(jsonObject.getString("responseValue"));
            JSONObject reqVC = jsonObject.getJSONObject("requestVectorClock");
            if (reqVC.length() != 0) {
                for (Iterator it = reqVC.keys(); it.hasNext(); ) {
                    String i = it.next().toString();
                    this.requestVectorClock.put(Integer.parseInt(i), Timestamp.valueOf(reqVC.getString(i)));
                }
            }
            JSONObject resVC = jsonObject.getJSONObject("responseVectorClock");
            if (resVC.length() != 0) {
                for (Iterator it = resVC.keys(); it.hasNext(); ) {
                    String i = it.next().toString();
                    this.responseVectorClock.put(Integer.parseInt(i), Timestamp.valueOf(resVC.getString(i)));
                }
            }
            JSONObject reqW = jsonObject.getJSONObject("requestWrites");
            if (reqW.length() != 0) {
                for (Iterator it = reqW.keys(); it.hasNext(); ) {
                    String i = it.next().toString();
                    this.requestWrites.put(Integer.parseInt(i), new ArrayList<>());
                    JSONArray jsonArray = reqW.getJSONArray(i);
                    for (int j = 0; j < jsonArray.length(); j++){
                        JSONObject jsonObject1 = jsonArray.getJSONObject(j);
                        this.requestWrites.get(Integer.parseInt(i)).add(new MRManager.Write(Timestamp.valueOf(jsonObject1.getString("ts")),
                                jsonObject1.getString("statement"), Integer.parseInt(jsonObject1.getString("node"))));
                    }
                }
            }
            JSONObject resW = jsonObject.getJSONObject("responseWrites");
            if (resW.length() != 0) {
                for (Iterator it = resW.keys(); it.hasNext(); ) {
                    String i = it.next().toString();
                    this.responseWrites.put(Integer.parseInt(i), new ArrayList<>());
                    JSONArray jsonArray = resW.getJSONArray(i);
                    for (int j = 0; j < jsonArray.length(); j++){
                        JSONObject jsonObject1 = jsonArray.getJSONObject(j);
                        this.responseWrites.get(Integer.parseInt(i)).add(new MRManager.Write(Timestamp.valueOf(jsonObject1.getString("ts")),
                                jsonObject1.getString("statement"), Integer.parseInt(jsonObject1.getString("node"))));
                    }
                }
            }
            this.setWritesTo(Timestamp.valueOf(jsonObject.getString("writesTo")));
            this.setWritesFrom(Timestamp.valueOf(jsonObject.getString("writesFrom")));
            this.setDestination(jsonObject.getInt("destination"));
            this.setSource(jsonObject.getInt("source"));
        }
//        else{
//            this = new FailureDetectionPacket(jsonObject);
//        }
    }
    public enum MRPacketType implements IntegerPacketType {
        READ("READ", 1401),
        WRITE("WRITE", 1402),
        FWD("FWD", 1403),
        FWD_ACK("FWD_ACK", 1404),
        FAILURE_DETECT("FAILURE_DETECT", 1405),
        RESPONSE("RESPONSE", 1406),
        ;
        String label;
        int number;
        private static HashMap<String, MRPacketType> labels = new HashMap<String, MRPacketType>();
        private static HashMap<Integer, MRPacketType> numbers = new HashMap<Integer, MRPacketType>();
        MRPacketType(String s, int t) {
            this.label = s;
            this.number = t;
        }
        static {
            for (MRRequestPacket.MRPacketType type: MRRequestPacket.MRPacketType.values()) {
                if (!MRRequestPacket.MRPacketType.labels.containsKey(type.label)
                        && !MRRequestPacket.MRPacketType.numbers.containsKey(type.number)) {
                    MRRequestPacket.MRPacketType.labels.put(type.label, type);
                    MRRequestPacket.MRPacketType.numbers.put(type.number, type);
                } else {
                    assert(false): "Duplicate or inconsistent enum type for ChainPacketType";
                }
            }
        }
        @Override
        public int getInt() {
            return this.number;
        }
        public static MRRequestPacket.MRPacketType getMRPacketType(int type){
            return MRRequestPacket.MRPacketType.numbers.get(type);
        }

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
    public MRPacketType getType() {
        return this.packetType;
    }
    public void setServiceName(String name){
        this.serviceName = name;
    }

    public HashMap<Integer, Timestamp> getRequestVectorClock() {
        return requestVectorClock;
    }

    public HashMap<Integer, ArrayList<MRManager.Write>> getRequestWrites() {
        return requestWrites;
    }

    public void setRequestWrites(HashMap<Integer, ArrayList<MRManager.Write>> requestWrites) {
        this.requestWrites = requestWrites;
    }

    public HashMap<Integer, ArrayList<MRManager.Write>> getResponseWrites() {
        return responseWrites;
    }

    public void setResponseWrites(HashMap<Integer, ArrayList<MRManager.Write>> responseWrites) {
        this.responseWrites = responseWrites;
    }

    public void setRequestVectorClock(HashMap<Integer, Timestamp> requestVectorClock) {
        this.requestVectorClock = requestVectorClock;
    }
    public void addResponseWrites(Integer nodeID, Timestamp ts, String statement) {
        if(!this.responseWrites.containsKey(nodeID)){
            this.responseWrites.put(nodeID, new ArrayList<>());
        }
        this.responseWrites.get(nodeID).add(new MRManager.Write(ts, statement, nodeID));
    }

    public Timestamp getWritesTo() {
        return writesTo;
    }

    public void setWritesTo(Timestamp writesTo) {
        this.writesTo = writesTo;
    }

    public Timestamp getWritesFrom() {
        return writesFrom;
    }

    public void setWritesFrom(Timestamp writesFrom) {
        this.writesFrom = writesFrom;
    }

    public HashMap<Integer, Timestamp> getResponseVectorClock() {
        return responseVectorClock;
    }

    public void setResponseVectorClock(HashMap<Integer, Timestamp> responseVectorClock) {
        this.responseVectorClock = responseVectorClock;
    }

    public String getResponseValue() {
        return responseValue;
    }

    public void setResponseValue(String responseValue) {
        this.responseValue = responseValue;
    }

    public InetSocketAddress getClientSocketAddress() {
        return clientSocketAddress;
    }

    public void setClientSocketAddress(InetSocketAddress clientSocketAddress) {
        this.clientSocketAddress = clientSocketAddress;
    }

    public MRPacketType getPacketType() {
        return packetType;
    }

    public void setPacketType(MRPacketType packetType) {
        this.packetType = packetType;
    }

    @Override
    public ClientRequest getResponse() {
//        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! getResponse is called!!!!!!!!!!!!");

        MRRequestPacket reply = new MRRequestPacket(this.requestID,
                MRRequestPacket.MRPacketType.RESPONSE, this);
//        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Response value is "+response+"!!!!!!!!!!!!");
        reply.responseValue = this.responseValue;
        reply.responseVectorClock  = this.responseVectorClock;
        reply.responseWrites = this.responseWrites;
        reply.source = this.source;
        System.out.println("Respnse:------------"+this.responseVectorClock);
        return reply;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return this.getType();
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("requestID", this.requestID);
        jsonObject.put("requestValue", this.requestValue);
        jsonObject.put("serviceName", this.serviceName);
        jsonObject.put("responseValue", this.responseValue);
        jsonObject.put("requestVectorClock", this.requestVectorClock);
//        System.out.println("Before converting: "+this.responseVectorClock);
        jsonObject.put("responseVectorClock", this.responseVectorClock);
        JSONObject reqWrites = new JSONObject();
        for(Integer i: this.requestWrites.keySet()){
            JSONArray jsonArray = new JSONArray();
            for(MRManager.Write write: this.requestWrites.get(i)){
                jsonArray.put(write.toJSONObjectImpl());
            }
            reqWrites.put(String.valueOf(i), jsonArray);
        }
        jsonObject.put("requestWrites", reqWrites);
        JSONObject resWrites = new JSONObject();
        for(Integer i: this.responseWrites.keySet()){
            JSONArray jsonArray = new JSONArray();
            for(MRManager.Write write: this.responseWrites.get(i)){
                jsonArray.put(write.toJSONObjectImpl());
            }
            resWrites.put(String.valueOf(i), jsonArray);
        }
        jsonObject.put("responseWrites", resWrites);
        jsonObject.put("writesTo", this.writesTo);
        jsonObject.put("writesFrom", this.writesFrom);
        jsonObject.put("destination", this.destination);
        jsonObject.put("source", this.source);
        jsonObject.put("clientSocketAddress", this.clientSocketAddress);
        jsonObject.put("type", this.packetType.getInt());
//        jsonObject.put("")
        return jsonObject;
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
