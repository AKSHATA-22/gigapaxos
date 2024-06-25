package edu.umass.cs.consistency.ClientCentric;

import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;

public class FailureDetectionPacket<NodeIDType> extends CCRequestPacket {

    private static enum Keys {
        SNDR, RCVR, MODE, SADDR
    };

    /**
     * Node ID of sender sending this keep-alive packet.
     */
    public final NodeIDType senderNodeID;
    /**
     * Destination to which the keep-alive being sent.
     */
    private final NodeIDType responderNodeID;
    /**
     * A status flag that is currently not used for anything.
     */
    private final boolean status;

    /** Need this if sender's address is different from that in node config.
     */
    private InetSocketAddress saddr=null;

    public FailureDetectionPacket(NodeIDType senderNodeID,
                                  NodeIDType responderNodeID, boolean status) {
        super(0, CCPacketType.FAILURE_DETECT, null);
        this.senderNodeID = senderNodeID;
        this.responderNodeID = responderNodeID;
        this.status = status;
    }

    public FailureDetectionPacket(JSONObject json,
                                  Stringifiable<NodeIDType> unstringer) throws JSONException {
        super(json);
        this.senderNodeID = unstringer.valueOf(json.getString(FailureDetectionPacket.Keys.SNDR
                .toString()));
        this.responderNodeID = unstringer.valueOf(json.getString(FailureDetectionPacket.Keys.RCVR
                .toString()));
        this.status = json.getBoolean(FailureDetectionPacket.Keys.MODE.toString());
        this.saddr = MessageNIOTransport.getSenderAddress(json);
    }

    @Override
    public JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject json = new JSONObject();
        json.put(FailureDetectionPacket.Keys.MODE.toString(), status);
        json.put(Keys.SNDR.toString(), senderNodeID);
        json.put(Keys.RCVR.toString(), responderNodeID);
        json.putOpt(FailureDetectionPacket.Keys.SADDR.toString(), this.saddr);
        json.put("type", this.getPacketType().getInt());
        json.put("requestID", this.getRequestID());
        json.put("serviceName", CCManager.getDefaultServiceName());
        return json;
    }

    public InetSocketAddress getSender() {
        return this.saddr;
    }

    protected String getSummaryString() {
        return "";
    }

}
