package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DynamoCoordinator<NodeIDType>
        extends AbstractReplicaCoordinator<NodeIDType> {
    private final DynamoManager<NodeIDType> dynamoManager;
    public DynamoCoordinator(Replicable app, NodeIDType myID,
                             Stringifiable<NodeIDType> unstringer,
                             Messenger<NodeIDType, ?> niot) {
        super(app, niot);
        assert (niot instanceof JSONMessenger);
        this.dynamoManager = new DynamoManager(myID, unstringer,
                (JSONMessenger<NodeIDType>) niot, this, "logs/DAGLogs.log",
                true);
    }
    private static Set<IntegerPacketType> requestTypes = null;
    @Override
    public Set<IntegerPacketType> getRequestTypes() {

        if(requestTypes!=null) return requestTypes;
        // FIXME: get request types from a proper app
        Set<IntegerPacketType> types = this.app.getRequestTypes();

        if (types==null) types= new HashSet<IntegerPacketType>();

        for (IntegerPacketType type: DynamoRequestPacket.DynamoPacketType.values())
            types.add(type);

        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        return requestTypes = types;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback) throws IOException, RequestParseException {
        // coordinate the request by sending in the respective quorum
//        System.out.println("In coordinate request");
        return this.dynamoManager.propose(request.getServiceName(), request, callback)!= null;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        System.out.println(">>>>> Create quorum: "+serviceName+", on "+this.getMyID());

        return this.dynamoManager.createReplicatedQuorumForcibly(
                serviceName, epoch, nodes, this, state);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        return this.dynamoManager.deleteReplicatedQuorum(serviceName, epoch);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return this.dynamoManager.getReplicaGroup(serviceName);
    }
    @Override
    public Integer getEpoch(String name) {
        return this.dynamoManager.getVersion(name);
    }


}
