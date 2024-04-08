package edu.umass.cs.consistency.MonotonicReads;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class MRApp implements Replicable {
    public String name = "MRReplicationApp";
    private HashMap<String, String> board = new HashMap<>();
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        System.out.println("In get request of app");
        MRRequestPacket mrRequestPacket = null;
        try {
            mrRequestPacket = new MRRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            ReconfigurationConfig.getLogger().info(
                    "Unable to parse request " + stringified);
            throw new RequestParseException(je);
        }
        return mrRequestPacket;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(MRRequestPacket.MRPacketType.values()));
    }

    @Override
    public boolean execute(Request request) {
        System.out.println("In execute request of MR Replication");
        if (request instanceof MRRequestPacket) {
//            Request will be of type(C for create and U for update): C CIRCLE01 1,1; C TRIANGLE02 2,4; U CIRCLE01 1,4
            if (((MRRequestPacket) request).getType() == MRRequestPacket.MRPacketType.WRITE) {
                String req = ((MRRequestPacket) request).getRequestValue();
                System.out.println("WRITE request "+req);
                String[] reqArray = req.split(" ");
                try {
                    if (reqArray[0].equals("U") & !this.board.containsKey(reqArray[1])) {
                        ((MRRequestPacket) request).setResponseValue("Update cannot be performed as the element does not exist");
                    }
                    else {
                        this.board.put(reqArray[1], reqArray[2]);
                        ((MRRequestPacket) request).setResponseValue(this.board+"");
                    }
                }
                catch (Exception e){
                    ((MRRequestPacket) request).setResponseValue(e.toString());
                }
            }
            else if (((MRRequestPacket) request).getType() == MRRequestPacket.MRPacketType.READ){
                System.out.println("In MR App for READ request");
                ((MRRequestPacket) request).setResponseValue(this.board+"");
            }

            System.out.println("After execution: "+this.board.toString());
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
        System.out.println("In checkpoint");
        return this.board.toString();
    }

    @Override
    public boolean restore(String name, String state) {
        System.out.println("In restore");
        this.board = new HashMap<String, String>();
        if(state != null){
            try{
                for (String keyValuePair : state.split(", ")) {
                    String[] keyValue = keyValuePair.split("=");
                    this.board.put(keyValue[0], keyValue[1]);
                }
            }
            catch (Exception e){
                System.out.println("Exception");
                e.printStackTrace();
            }
        }
        return true;
    }
}

