package edu.umass.cs.consistency.Quorum;

import edu.umass.cs.chainreplication.chainpackets.ChainPacket;
import edu.umass.cs.chainreplication.chainpackets.ChainRequestPacket;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.HashMap;

public class QuorumRequestPacket extends JSONPacket implements Request, ClientRequest, Byteable  {
    public final long requestID;
    private HashMap<String, String> responseValue = null;
    public final String requestValue;
    private InetSocketAddress clientSocketAddress;
    private InetSocketAddress listenSocketAddress;
    private QuorumPacketType packetType;
    private String quorumID = null;
    private int version = -1;
    private int slot;
    /**
     * To avoid potential conflict with existing {@link PaxosPacket} PaxosPacketType and {@Link ChainPacket} ChainPacketType,
     * ChainPacketType starts from 1200 to 1300
     */
    public enum QuorumPacketType implements IntegerPacketType {

        REQUEST("QUORUM_REQ", 1201),
        READ("QUORUM_READ", 1202),
        WRITE("QUORUM_WRITE", 1203),
        WRITEFORWARD("QUORUM_WRITE_FWD", 1204),
        READFORWARD("QUORUM_READ_FWD", 1205),
        WRITEACK("QUORUM_WRITE_ACK", 1206),
        READACK("QUORUM_READ_ACK", 1207),
        RESPONSE("RESPONSE", 1208),

        QUORUM_PACKET("QUORUM_PACKET", 1299),
        ;
        private final String label;
        private final int number;
        QuorumPacketType(String s, int t) {
            this.label = s;
            this.number = t;
        }
        @Override
        public int getInt() {
            return this.number;
        }
        public String getLabel(){
            return this.label;
        }
    }
    public QuorumRequestPacket(long reqID, String value,
                               QuorumRequestPacket req){
        super(QuorumPacketType.QUORUM_PACKET);

        this.packetType = QuorumPacketType.REQUEST;
        this.requestID = reqID;
        this.requestValue = value;

        if(req == null)
            return;

        this.responseValue = req.responseValue;

        this.clientSocketAddress = req.clientSocketAddress;
        this.listenSocketAddress = req.listenSocketAddress;
    }

    public void setQuorumID(String quorumID){
        this.quorumID = quorumID;
    }
    public void setVersion(Integer version){
        this.version = version;
    }
    public void setPacketType(QuorumPacketType packetType){
        this.packetType = packetType;
    }
    public void addResponse(String key, String value){
        this.responseValue.put(key, value);
    }
    @Override
    public InetSocketAddress getClientAddress() {
        return ClientRequest.super.getClientAddress();
    }

    @Override
    public ClientRequest getResponse() {
        return null;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return null;
    }
    public QuorumPacketType getType() {
        return this.packetType;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        return null;
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public byte[] toBytes() {
        //to be implemented
        return null;
    }

    @Override
    public long getRequestID() {
        return 0;
    }
}
