package edu.umass.cs.consistency.Quorum;

import edu.umass.cs.chainreplication.ChainManager;
import edu.umass.cs.chainreplication.ReplicatedChainStateMachine;
import edu.umass.cs.chainreplication.chainpackets.ChainPacket;
import edu.umass.cs.chainreplication.chainpackets.ChainRequestPacket;
import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.gigapaxos.paxosutil.PaxosMessenger;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Util;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QuorumManager<NodeIDType> {

    private final PaxosMessenger<NodeIDType> messenger; // messaging
    private final int myID;
    private final Replicable myApp;
    private final HashMap<String, ReplicatedQuorumStateMachine> replicatedQuorums;

    //    Maps the write version/state of this node for a quorumID
    private HashMap<Integer, Integer> version;
    private final Stringifiable<NodeIDType> unstringer;
    // a map of NodeIDType objects to integers
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
//    Maps the reqestID to QuorumRequestAndCallback object
    private HashMap<Long, QuorumRequestAndCallback> requestsReceived;
    private final LargeCheckpointer largeCheckpointer;
    private static final Logger log = Logger.getLogger(ReconfigurationConfig.class.getName());

    public <NodeIDType> QuorumManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
                                      InterfaceNIOTransport<NodeIDType, JSONObject> niot, Replicable instance,
                                      String logFolder, boolean enableNullCheckpoints) {
        this.myID = this.integerMap.put(id);

        this.unstringer = unstringer;

        this.largeCheckpointer = new LargeCheckpointer(logFolder,
                id.toString());
        this.myApp = LargeCheckpointer.wrap(instance, largeCheckpointer);

        this.replicatedQuorums = new HashMap<>();

        this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));
    }
    public static class QuorumRequestAndCallback {
        protected QuorumRequestPacket quorumRequestPacket;
        final ExecutedCallback callback;
        protected Integer numOfAcksReceived = 0;

        QuorumRequestAndCallback(QuorumRequestPacket quorumRequestPacket, ExecutedCallback callback){
            this.quorumRequestPacket = quorumRequestPacket;
            this.callback = callback;
        }

        @Override
        public String toString(){
            return this.quorumRequestPacket +" ["+ callback+"]";
        }
        public Integer incrementAck(){
            this.numOfAcksReceived += 1;
            return this.numOfAcksReceived;
        }
        public Integer getNumOfAcksReceived(){
            return this.numOfAcksReceived;
        }
    }
    private void handleQuorumPacket(QuorumRequestPacket qp, ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){

        QuorumRequestPacket.QuorumPacketType packetType = qp.getType();

        switch(packetType) {
            case READ:
                // client -> node
                handleReadRequest(qp, rqsm, callback);
                break;
            case READFORWARD:
                // node -> read_quorum_node
                handleReadForward(qp, rqsm);
                break;
            case READACK:
                // read_quorum_node -> node
                handleReadAck(qp, rqsm);
                break;
            case WRITE:
                // client -> node
                handleWriteRequest(qp, rqsm, callback);
                break;
            case WRITEFORWARD:
                // node -> write_quorum_node
                handleWriteForward(qp, rqsm);
                break;
            case WRITEACK:
                // write_quorum_node -> node
                handleWriteAck(qp, rqsm);
                break;
            case RESPONSE:
                // node -> client
                handleResponse(qp, rqsm);
                break;
            default:
                break;
        }

    }
    private void handleReadRequest(QuorumRequestPacket qp,
                            ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){

        this.requestsReceived.putIfAbsent(qp.getRequestID(), new QuorumRequestAndCallback(qp, callback));
//        Send read_forward request to atleast read_quorum number of nodes
        ArrayList<Integer> sent = new ArrayList<Integer>();
        for (int i = 0; i < rqsm.getReadQuorum(); i++) {
            Integer num = getRandomNumber(0, rqsm.getQuorumMembers().size()-1);
            while (!sent.contains(num)){
                num = getRandomNumber(0, rqsm.getQuorumMembers().size()-1);
            }
            sent.add(num);
            qp.setPacketType(QuorumRequestPacket.QuorumPacketType.READFORWARD);
            this.sendRequest(qp, rqsm.getQuorumMembers().get(num));
        }
//        Save the request and number of acks received in a hashmap
//        Once the number of acks equals the read_quorum nodes check the version of each ack
//        return the value of the ack with highest version
//        write into own value if self.version is not equal to highest version
    }
    public void handleReadForward(QuorumRequestPacket qp, ReplicatedQuorumStateMachine rqsm){
//        return the value from underlying app and the version from version hashmap
        Request request = getInterfaceRequest(this.myApp, qp.requestValue);
        this.myApp.execute(request, false);
//        qp.addResponse("value", request.getRe);
    }
    private static Request getInterfaceRequest(Replicable app, String value) {
        try {
            return app.getRequest(value);
        } catch (RequestParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    public int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }
    private void sendRequest(QuorumRequestPacket qp,
                             int nodeID){
        GenericMessagingTask<NodeIDType,?> gTask = null;
        try {
            // forward to nodeID
            gTask = new GenericMessagingTask(this.integerMap.get(nodeID),
                    qp);
            this.messenger.send(gTask);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String propose(String quorumID, Request request,
                          ExecutedCallback callback) {

        QuorumRequestPacket quorumRequestPacket = this.getQuorumRequestPacket(request);

        boolean matched = false;

        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);

        if (rqsm != null) {
            matched = true;
            quorumRequestPacket.setQuorumID(quorumID);
            quorumRequestPacket.setVersion(rqsm.getVersion());


            this.handleQuorumPacket(quorumRequestPacket, rqsm, callback);
        } else {
            System.out.println("The given quorumID has no state machine associated");
        }


        return matched ? rqsm.getQuorumID() : null;
    }
    private QuorumRequestPacket getQuorumRequestPacket(Request request){

        if (request instanceof QuorumRequestPacket) {
            return (QuorumRequestPacket) request;
        }
        System.out.println("The request should be an instance of QuorunRequestPacket. Invalid request.");
        return null;
    }

    public boolean createReplicatedQuorumForcibly(String quorumID, int version,
                                                 Set<NodeIDType> nodes, Replicable app,
                                                 String state){
        return this.createReplicatedQuorumFinal(quorumID, version, nodes, app, state) != null;
    }

    private synchronized ReplicatedQuorumStateMachine createReplicatedQuorumFinal(
            String quorumID, int version, Set<NodeIDType> nodes,
            Replicable app, String initialState){
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm != null)
            return rqsm;
        try {
            rqsm = new ReplicatedQuorumStateMachine(quorumID, version, myID,
                    this.integerMap.put(nodes), app != null ? app : this.myApp,
                    initialState, this);
            System.out.println("Creating new Replicated Quorum State Machine: "+ rqsm);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        this.putInstance(quorumID, rqsm);
        this.integerMap.put(nodes);

        return rqsm;
    }
    public Set<NodeIDType> getReplicaGroup(String quorumID) {
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm == null)
            return null;
        return this.integerMap.getIntArrayAsNodeSet(rqsm.getQuorumMembers());
    }
    public boolean deleteReplicatedQuorum(String quorumID, int epoch){
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if(rqsm == null)
            return true;
        if(rqsm.getVersion() > epoch) {
            return false;
        }
        return this.removeInstance(quorumID);
    }
    private boolean removeInstance(String quorumID) {
        return this.replicatedQuorums.remove(quorumID) != null;
    }
    private void putInstance(String quorumID, ReplicatedQuorumStateMachine rcsm){
        this.replicatedQuorums.put(quorumID, rcsm);
    }

    private ReplicatedQuorumStateMachine getInstance(String quorumID){
        return this.replicatedQuorums.get(quorumID);
    }

    public Integer getVersion(String quorumID) {
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if ( rqsm != null)
            return (int) rqsm.getVersion();
        return -1;
    }
}
