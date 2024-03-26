package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.Quorum.QuorumManager;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
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
                (JSONMessenger<NodeIDType>) niot, this, null,
                true);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return null;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback) throws IOException, RequestParseException {
        return false;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        return false;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        return false;
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return null;
    }
}
