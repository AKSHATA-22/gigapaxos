package edu.umass.cs.consistency.MonotonicReads;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;

public class MRRequestPacket extends JSONPacket implements ReplicableRequest, ClientRequest {
    public final long requestID;
    //    Maps the versionVector hashmap to the final response string
    private String requestValue = null;
    private String serviceName = null;
    private HashMap<Integer, Timestamp> requestVectorClock = new HashMap<Integer, Timestamp>();
    private HashMap<Integer, Timestamp> responseVectorClock = new HashMap<Integer, Timestamp>();
    private HashMap<Timestamp, String> requestWrites = new HashMap<>();
    private HashMap<Timestamp, String> responseWrites = new HashMap<>();
    private String responseValue = null;
    private Timestamp writesTo = null;
    private Timestamp writesFrom = null;
    private int destination = -1;
    private int source = -1;
    private InetSocketAddress clientSocketAddress = null;
    private MRPacketType packetType;

    public MRRequestPacket(long reqID, MRPacketType reqType, String serviceName, String value, HashMap<Integer, Timestamp> requestVectorClock, HashMap<Timestamp, String> requestWrites){
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
        this.setServiceName(jsonObject.getString("serviceName"));
        this.setRequestValue(jsonObject.getString("requestValue"));
        this.setResponseValue(jsonObject.getString("responseValue"));
        JSONObject reqVC = jsonObject.getJSONObject("requestVectorClock");
        if (reqVC.length() != 0) {
            for (Iterator it = reqVC.keys(); it.hasNext(); ) {
                int i = Integer.parseInt(it.next().toString());
                this.requestVectorClock.put(i, new Timestamp(reqVC.getLong(String.valueOf(i))));
            }
        }
        JSONObject resVC = jsonObject.getJSONObject("responseVectorClock");
        if (resVC.length() != 0) {
            for (Iterator it = reqVC.keys(); it.hasNext(); ) {
                int i = Integer.parseInt(it.next().toString());
                this.responseVectorClock.put(i, new Timestamp(resVC.getLong(String.valueOf(i))));
            }
        }
        JSONObject reqW = jsonObject.getJSONObject("requestWrites");
        if (reqW.length() != 0) {
            for (Iterator it = reqW.keys(); it.hasNext(); ) {
                String i = it.next().toString();
                this.requestWrites.put(new Timestamp(Long.parseLong(i)), reqW.getString(i));
            }
        }
        JSONObject resW = jsonObject.getJSONObject("responseWrites");
        if (resW.length() != 0) {
            for (Iterator it = resW.keys(); it.hasNext(); ) {
                String i = it.next().toString();
                this.responseWrites.put(new Timestamp(Long.parseLong(i)), resW.getString(i));
            }
        }
        this.setWritesTo(new Timestamp(Long.parseLong(jsonObject.getString("writesTo"))));
        this.setWritesFrom(new Timestamp(Long.parseLong(jsonObject.getString("writesFrom"))));
        this.setDestination(jsonObject.getInt("destination"));
        this.setSource(jsonObject.getInt("source"));
    }
    public enum MRPacketType implements IntegerPacketType {
        READ("READ", 1401),
        WRITE("WRITE", 1402),
        FWD("FWD", 1403),
        FWD_ACK("FWD_ACK", 1404),
        RESPONSE("RESPONSE", 1405),
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

    public void setRequestVectorClock(HashMap<Integer, Timestamp> requestVectorClock) {
        this.requestVectorClock = requestVectorClock;
    }

    public HashMap<Timestamp, String> getRequestWrites() {
        return requestWrites;
    }

    public void setRequestWrites(HashMap<Timestamp, String> requestWrites) {
        this.requestWrites = requestWrites;
    }

    public HashMap<Timestamp, String> getResponseWrites() {
        return responseWrites;
    }

    public void setResponseWrites(HashMap<Timestamp, String> responseWrites) {
        this.responseWrites = responseWrites;
    }
    public void addResponseWrites(Timestamp ts, String statement) {
        this.responseWrites.put(ts, statement);
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
        jsonObject.put("responseVectorClock", this.responseVectorClock);
        jsonObject.put("requestWrites", this.requestWrites);
        jsonObject.put("responseWrites", this.responseWrites);
        jsonObject.put("writesTo", this.writesTo);
        jsonObject.put("writesFrom", this.writesFrom);
        jsonObject.put("destination", this.destination);
        jsonObject.put("source", this.source);
        jsonObject.put("clientSocketAddress", this.clientSocketAddress);
        jsonObject.put("type", this.packetType.getInt());
        return jsonObject;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }
}
