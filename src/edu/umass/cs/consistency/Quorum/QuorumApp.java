package edu.umass.cs.consistency.Quorum;

import edu.umass.cs.gigapaxos.examples.adder.StatefulAdderApp;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class QuorumApp extends StatefulAdderApp {
    public String name = "QuorumReplicationApp";
    protected int total = 20;
    protected int version = 0;
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        System.out.println("In get request of app");
        QuorumRequestPacket quorumRequestPacket = null;
        try {
            quorumRequestPacket = new QuorumRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            ReconfigurationConfig.getLogger().info(
                    "Unable to parse request " + stringified);
            throw new RequestParseException(je);
        }
        return quorumRequestPacket;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(QuorumRequestPacket.QuorumPacketType.values()));
    }

    @Override
    public boolean execute(Request request) {
        System.out.println("In execute request of Quorum Replication");
        if (request instanceof QuorumRequestPacket) {
            if (((QuorumRequestPacket) request).getType() != QuorumRequestPacket.QuorumPacketType.WRITE ||
                    ((QuorumRequestPacket) request).getType() != QuorumRequestPacket.QuorumPacketType.WRITEFORWARD) {
                System.out.println("Returning total as: " + this.total);
                ((QuorumRequestPacket) request).addResponse("value", String.valueOf(total));
                ((QuorumRequestPacket) request).addResponse("version", String.valueOf(version));
            }
            else{
                System.out.println("Packet type received: "+ ((QuorumRequestPacket) request).getType());
                System.out.println("In Quorum for write request");
                this.version = Integer.parseInt(((QuorumRequestPacket) request).getRequestValue().get("version"));
                this.total = Integer.parseInt(((QuorumRequestPacket) request).getRequestValue().get("total"));
            }
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
        return this.total+"";
    }

    @Override
    public boolean restore(String name, String state) {
        if(state == null){
            this.total = 0;
        }
        else{
            try{
                this.total = Integer.valueOf(state);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        return true;
    }

}
