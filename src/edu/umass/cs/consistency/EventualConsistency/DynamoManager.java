package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.consistency.Quorum.ReplicatedQuorumStateMachine;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.gigapaxos.paxosutil.PaxosMessenger;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DynamoManager<NodeIDType> {
    private final PaxosMessenger<NodeIDType> messenger; // messaging
    private final int myID;
    private final Replicable myApp;
    private final HashMap<String, ReplicatedQuorumStateMachine> replicatedQuorums;

    //    Maps the quorumID to the vectorClock hashmap
    private HashMap<String, HashMap<Integer, Integer>> vectorClock = new HashMap<>();
    private final Stringifiable<NodeIDType> unstringer;
    // a map of NodeIDType objects to integers
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    private Timestamp lastWriteTS = new Timestamp(0);
    //    Maps the reqestID to QuorumRequestAndCallback object
    private HashMap<Long, DynamoRequestAndCallback> requestsReceived = new HashMap<Long, DynamoRequestAndCallback>();
    private ArrayList<String> quorums = new ArrayList<String>();
    private final LargeCheckpointer largeCheckpointer;
    private static final Logger log = Logger.getLogger(ReconfigurationConfig.class.getName());

    public static final Class<?> application = DynamoApp.class;
    public static final String getDefaultServiceName() {
        return application.getSimpleName() + "0";
    }
    public DynamoManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
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
    public static class DynamoRequestAndCallback {
        protected DynamoRequestPacket dynamoRequestPacket;
        final ExecutedCallback callback;
        protected Integer numOfAcksReceived = 0;

        DynamoRequestAndCallback(DynamoRequestPacket dynamoRequestPacket, ExecutedCallback callback){
            this.dynamoRequestPacket = dynamoRequestPacket;
            this.callback = callback;
        }

        @Override
        public String toString(){
            return this.dynamoRequestPacket +" ["+ callback+"]";
        }
        public void reset(){
            this.numOfAcksReceived = 0;
        }
        public Integer incrementAck(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm){
            this.numOfAcksReceived += 1;
            if (qp.getType() == DynamoRequestPacket.DynamoPacketType.GET_ACK){
                System.out.println("GET ACK received----------");
                this.reconcile(qp.getResponsePacket().getVectorClock(), qp.getResponsePacket().getValue(), rqsm, qp.getTimestamp());
            }
            return this.numOfAcksReceived;
        }
        public void reconcile(HashMap<Integer, Integer> vectorClock, int value, ReplicatedQuorumStateMachine rqsm, Timestamp ts){
            System.out.println("========================================================"+this.dynamoRequestPacket.getResponsePacket());
            if (!this.dynamoRequestPacket.getResponsePacket().getVectorClock().isEmpty()){
                boolean greater = true;
                boolean smaller = true;
                for (int i = 0; i < vectorClock.size(); i++) {
                    if(this.dynamoRequestPacket.getResponsePacket().getVectorClock().get(rqsm.getQuorumMembers().get(i)) > vectorClock.get(rqsm.getQuorumMembers().get(i))){
                        greater &= true;
                        smaller &= false;
                    }
                    else if(this.dynamoRequestPacket.getResponsePacket().getVectorClock().get(rqsm.getQuorumMembers().get(i)) < vectorClock.get(rqsm.getQuorumMembers().get(i))){
                        greater &= false;
                        smaller &= true;
                    }
                }
                if (smaller || greater){
                    if (smaller) {
                        this.dynamoRequestPacket.getResponsePacket().setVectorClock(vectorClock);
                        this.dynamoRequestPacket.getResponsePacket().setValue(value);
                        this.dynamoRequestPacket.setTimestamp(ts);
                    }
                }
                else {
                    if(this.dynamoRequestPacket.getTimestamp().compareTo(ts) < 0){
                        this.dynamoRequestPacket.getResponsePacket().setVectorClock(vectorClock);
                        this.dynamoRequestPacket.getResponsePacket().setValue(value);
                        this.dynamoRequestPacket.setTimestamp(ts);
                    }
                }
            }
            else {
                this.dynamoRequestPacket.setResponsePacket(new DynamoRequestPacket.DynamoPacket(vectorClock, value));
                this.dynamoRequestPacket.setTimestamp(ts);
            }
        }
        public Integer getNumOfAcksReceived(){
            return this.numOfAcksReceived;
        }
        public DynamoRequestPacket getResponsePacket(){
            System.out.println("Returning resp pckt");
            this.dynamoRequestPacket.setPacketType(DynamoRequestPacket.DynamoPacketType.RESPONSE);
            return this.dynamoRequestPacket;
        }
    }
    private void handleDynamoPacket(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){

        DynamoRequestPacket.DynamoPacketType packetType = qp.getType();

        switch(packetType){
            case PUT:
                // client -> node
                handlePutRequest(qp, rqsm, callback);
                break;
            case PUT_FWD:
                // node -> read_quorum_node
                handlePutForward(qp, rqsm);
                break;
            case GET:
                // read_quorum_node -> node
                handleGetRequest(qp, rqsm, callback);
                break;
            case GET_FWD:
                // node -> write_quorum_node
                handleGetForward(qp, rqsm);
                break;
            case PUT_ACK: case GET_ACK:
                // write_quorum_node -> node
                handleAck(qp, rqsm);
                break;
            default:
                break;
        }

    }
    private void handlePutRequest(DynamoRequestPacket qp,
                               ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new DynamoRequestAndCallback(qp, callback));
        Request request = getInterfaceRequest(this.myApp, qp.toString());
        this.myApp.execute(request, false);
        this.vectorClock.get(rqsm.getQuorumID()).put(this.myID, this.vectorClock.get(rqsm.getQuorumID()).get(this.myID)+1);
        this.lastWriteTS = new Timestamp(System.currentTimeMillis());
        qp.setRequestVectorClock(this.vectorClock.get(rqsm.getQuorumID()));
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.PUT_FWD);
//        Send request to all the quorum members except own
        for (int i = 0; i < rqsm.getQuorumMembers().size()-1; i++) {
            if (rqsm.getQuorumMembers().get(i) != this.myID) {
                qp.setSource(this.myID);
                qp.setDestination(rqsm.getQuorumMembers().get(i));
                this.sendRequest(qp, qp.getDestination());
            }
        }
    }
    private void handleGetRequest(DynamoRequestPacket qp,
                                  ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new DynamoRequestAndCallback(qp, callback));
        qp.setRequestVectorClock(this.vectorClock.get(rqsm.getQuorumID()));
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.GET_FWD);
//        Send request to all the quorum members
        for (int i = 0; i < rqsm.getQuorumMembers().size()-1; i++) {
            qp.setSource(this.myID);
            qp.setDestination(rqsm.getQuorumMembers().get(i));
            this.sendRequest(qp, qp.getDestination());
        }
    }

    public void handlePutForward(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm){
//        return the value from underlying app and the version from version hashmap
        if (qp.getRequestVectorClock().get(qp.getSource()) > this.vectorClock.get(rqsm.getQuorumID()).get(qp.getSource())) {
            Request request = getInterfaceRequest(this.myApp, qp.toString());
            this.myApp.execute(request, false);
            this.reconcileVectorClock(qp.getRequestVectorClock(), rqsm, qp.getSource());
            this.lastWriteTS = new Timestamp(System.currentTimeMillis());
        }
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.PUT_ACK);
        int dest = qp.getDestination();
        qp.setDestination(qp.getSource());
        qp.setSource(dest);
        qp.setResponsePacket(new DynamoRequestPacket.DynamoPacket(this.vectorClock.get(rqsm.getQuorumID()), -1));
        this.sendRequest(qp, qp.getDestination());
    }
    public void handleGetForward(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm){
//        return the value from underlying app and the version from version hashmap
        Request request = getInterfaceRequest(this.myApp, qp.toString());
        this.myApp.execute(request, false);
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.GET_ACK);
        qp.setTimestamp(this.lastWriteTS);
        int dest = qp.getDestination();
        qp.setDestination(qp.getSource());
        qp.setSource(dest);
        assert request != null;
        qp.setResponsePacket(new DynamoRequestPacket.DynamoPacket(this.vectorClock.get(rqsm.getQuorumID()), ((DynamoRequestPacket)request).getResponsePacket().getValue()));
        this.sendRequest(qp, qp.getDestination());
    }
    public void handleAck(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm){
//        append in the hashmap and check the total
        if (qp.getType() == DynamoRequestPacket.DynamoPacketType.PUT_ACK){
            try {
                if (this.requestsReceived.get(qp.getRequestID()).incrementAck(qp, rqsm) >= rqsm.getWriteQuorum()-1){
                    sendResponse(qp.getRequestID());
                }
            }
            catch (Exception e){
                log.log(Level.WARNING, e.toString());
            }
        } else if (qp.getType() == DynamoRequestPacket.DynamoPacketType.GET_ACK) {
            try {
                if (this.requestsReceived.get(qp.getRequestID()).incrementAck(qp, rqsm) >= rqsm.getReadQuorum()){
                    System.out.println("sending response for get--------");
                    sendResponse(qp.getRequestID());
                }
            }
            catch (Exception e){
                log.log(Level.WARNING, e.toString());
            }
        }
    }
    public void sendResponse(Long requestID){
        DynamoRequestAndCallback requestAndCallback = this.requestsReceived.get(requestID);
        if (requestAndCallback != null && requestAndCallback.callback != null) {

            requestAndCallback.callback.executed(requestAndCallback.getResponsePacket()
                    , true);
            this.requestsReceived.remove(requestID);

        } else {
            // can't find the request being queued in outstanding
            log.log(Level.WARNING, "QuorumManager.handleResponse received " +
                            "an ACK request {0} that does not match any enqueued request.",
                    new Object[]{requestID});
        }
    }
    private void sendRequest(DynamoRequestPacket qp,
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
    private void reconcileVectorClock(HashMap<Integer, Integer> requestClock, ReplicatedQuorumStateMachine rqsm, int node){
        ArrayList<Integer> members = rqsm.getQuorumMembers();
        this.vectorClock.get(rqsm.getQuorumID()).put(node, requestClock.get(node));
    }
    private static Request getInterfaceRequest(Replicable app, String value) {
        try {
            return app.getRequest(value);
        } catch (RequestParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String propose(String quorumID, Request request,
                          ExecutedCallback callback) {

        DynamoRequestPacket dynamoRequestPacket = this.getDynamoRequestPacket(request);
        boolean matched = false;

        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);

        if (rqsm != null) {
            matched = true;
            assert dynamoRequestPacket != null;
            dynamoRequestPacket.setQuorumID(quorumID);
            dynamoRequestPacket.setVersion(rqsm.getVersion());
            this.handleDynamoPacket(dynamoRequestPacket, rqsm, callback);
        } else {
            System.out.println("The given quorumID has no state machine associated");
        }


        return matched ? rqsm.getQuorumID() : null;
    }
    private DynamoRequestPacket getDynamoRequestPacket(Request request){
        try {
            return (DynamoRequestPacket) request;
        }
        catch (Exception e){
            System.out.println(e.toString());
        }
        return null;
    }
    private ReplicatedQuorumStateMachine getInstance(String quorumID){
        return this.replicatedQuorums.get(quorumID);
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
            quorums.add(quorumID);
            System.out.println("Creating new Replicated Quorum State Machine: "+ rqsm);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        this.putInstance(quorumID, rqsm);
        this.integerMap.put(nodes);
        this.putVectorClock(quorumID, rqsm);
        return rqsm;
    }
    public Set<NodeIDType> getReplicaGroup(String quorumID) {
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm == null)
            return null;
        return this.integerMap.getIntArrayAsNodeSet(rqsm.getQuorumMembersArray());
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
    private void putInstance(String quorumID, ReplicatedQuorumStateMachine rqsm){
        this.replicatedQuorums.put(quorumID, rqsm);
    }
    private void putVectorClock(String quorumID, ReplicatedQuorumStateMachine rqsm){
        this.vectorClock.put(quorumID, new HashMap<Integer, Integer>());
        for (int i = 0; i < rqsm.getQuorumMembers().size(); i++) {
            this.vectorClock.get(quorumID).put(rqsm.getQuorumMembers().get(i), 0);
        }
    }

    public Integer getVersion(String quorumID) {
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if ( rqsm != null)
            return (int) rqsm.getVersion();
        return -1;
    }
}
