package edu.umass.cs.consistency.Quorum;

import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp;
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
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
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
    private HashMap<Long, QuorumRequestAndCallback> requestsReceived = new HashMap<Long, QuorumRequestAndCallback>();
    private ArrayList<String> quorums = new ArrayList<String>();
    private final LargeCheckpointer largeCheckpointer;
    private static final Logger log = Logger.getLogger(ReconfigurationConfig.class.getName());

    public static final Class<?> application = QuorumApp.class;
    public static final String getDefaultServiceName() {
        return application.getSimpleName() + "0";
    }
    public QuorumManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
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
        protected Integer version = -1;
        protected Integer value = -1;

        QuorumRequestAndCallback(QuorumRequestPacket quorumRequestPacket, ExecutedCallback callback){
            this.quorumRequestPacket = quorumRequestPacket;
            this.callback = callback;
        }

        @Override
        public String toString(){
            return this.quorumRequestPacket +" ["+ callback+"]";
        }
        public void reset(){
            this.numOfAcksReceived = 0;
            this.version = -1;
            this.value = -1;
        }
        public Integer incrementAck(QuorumRequestPacket qp){
            this.numOfAcksReceived += 1;
            if (qp.getType() != QuorumRequestPacket.QuorumPacketType.WRITEACK && Integer.parseInt(qp.getResponseValue().get("version")) >= this.version){
                this.value = Integer.parseInt(qp.getResponseValue().get("value"));
                this.version = Integer.parseInt(qp.getResponseValue().get("version"));
            }
            return this.numOfAcksReceived;
        }
        public Integer getNumOfAcksReceived(){
            return this.numOfAcksReceived;
        }
        public QuorumRequestPacket getResponsePacket(){
            this.quorumRequestPacket.addResponse("version",this.version.toString());
            this.quorumRequestPacket.addResponse("value",this.value.toString());
            this.quorumRequestPacket.setPacketType(QuorumRequestPacket.QuorumPacketType.RESPONSE);
            return this.quorumRequestPacket;
        }
    }
    private void handleQuorumPacket(QuorumRequestPacket qp, ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){

        QuorumRequestPacket.QuorumPacketType packetType = qp.getType();

        switch(packetType) {
            case READ: case WRITE:
                // client -> node
                handleRequest(qp, rqsm, callback);
                break;
            case READFORWARD: case READFORWRITEFORWARD:
                // node -> read_quorum_node
                handleForward(qp);
                break;
            case READACK: case READFORWRITEACK:
                // read_quorum_node -> node
                handleReadAck(qp, rqsm);
                break;
            case WRITEFORWARD:
                // node -> write_quorum_node
                handleForward(qp);
                break;
            case WRITEACK:
                // write_quorum_node -> node
                handleWriteAck(qp, rqsm);
                break;
            default:
                break;
        }

    }
    private void handleRequest(QuorumRequestPacket qp,
                            ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){
        System.out.println(qp.getSummary());
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new QuorumRequestAndCallback(qp, callback));
//        Send request to all the quorum members
        for (int i = 0; i < rqsm.getQuorumMembers().size()-1; i++) {
            if(qp.getType() == QuorumRequestPacket.QuorumPacketType.READ){
                qp.setPacketType(QuorumRequestPacket.QuorumPacketType.READFORWARD);
            }
            else {
                qp.setPacketType(QuorumRequestPacket.QuorumPacketType.READFORWRITEFORWARD);
            }
            qp.setSource(this.myID);
            qp.setDestination(rqsm.getQuorumMembers().get(i));
            this.sendRequest(qp, qp.getDestination());
        }
    }
    public void handleForward(QuorumRequestPacket qp){
//        return the value from underlying app and the version from version hashmap
        System.out.println(qp.toString());
        Request request = getInterfaceRequest(this.myApp, qp.toString());
        this.myApp.execute(request, false);
        if(qp.getType() == QuorumRequestPacket.QuorumPacketType.READFORWARD){
            qp.setPacketType(QuorumRequestPacket.QuorumPacketType.READACK);
        }
        else if (qp.getType() == QuorumRequestPacket.QuorumPacketType.READFORWRITEFORWARD){
            qp.setPacketType(QuorumRequestPacket.QuorumPacketType.READFORWRITEACK);
        }
        else{
            qp.setPacketType(QuorumRequestPacket.QuorumPacketType.WRITEACK);
        }
        int dest = qp.getDestination();
        qp.setDestination(qp.getSource());
        qp.setSource(dest);
        assert request != null;
        qp.setResponseValue(((QuorumRequestPacket)request).getResponseValue());
        System.out.println(qp.toString());
        this.sendRequest(qp, qp.getDestination());
    }
    public void handleReadAck(QuorumRequestPacket qp, ReplicatedQuorumStateMachine rqsm){
//        append in the hashmap and check the total
        if (this.requestsReceived.get(qp.getRequestID()).incrementAck(qp) >= rqsm.getReadQuorum()){
            if(qp.getType() == QuorumRequestPacket.QuorumPacketType.READACK)
                sendResponse(qp.getRequestID());
            else
                sendWriteRequests(qp, rqsm);
        }
    }
    public void handleWriteAck(QuorumRequestPacket qp, ReplicatedQuorumStateMachine rqsm){
//        append in the hashmap and check the total
        if (this.requestsReceived.get(qp.getRequestID()).incrementAck(qp) >= rqsm.getWriteQuorum()){
            sendResponse(qp.getRequestID());
        }
    }
    public void sendWriteRequests(QuorumRequestPacket qp, ReplicatedQuorumStateMachine rqsm){
        qp.setPacketType(QuorumRequestPacket.QuorumPacketType.WRITEFORWARD);
        qp.addRequestEntry("version", this.requestsReceived.get(qp.getRequestID()).version.toString());
        qp.setResponseValue(new HashMap<>());
        this.requestsReceived.get(qp.getRequestID()).reset();
        for (int i = 0; i < rqsm.getQuorumMembers().size()-1; i++) {
            qp.setSource(this.myID);
            qp.setDestination(rqsm.getQuorumMembers().get(i));
            this.sendRequest(qp, qp.getDestination());
        }
    }
    public void sendResponse(Long requestID){
        QuorumRequestAndCallback requestAndCallback = this.requestsReceived.get(requestID);
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
        System.out.println("In propose");
        boolean matched = false;

        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);

        if (rqsm != null) {
            matched = true;
            assert quorumRequestPacket != null;
            quorumRequestPacket.setQuorumID(quorumID);
            quorumRequestPacket.setVersion(rqsm.getVersion());
            this.handleQuorumPacket(quorumRequestPacket, rqsm, callback);
        } else {
            System.out.println("The given quorumID has no state machine associated");
        }
        return matched ? rqsm.getQuorumID() : null;
    }
    private QuorumRequestPacket getQuorumRequestPacket(Request request){
        try {
            return (QuorumRequestPacket) request;
        }
        catch (Exception e){
            System.out.println(e.toString());
        }
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
            quorums.add(quorumID);
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

    public ArrayList<String> getQuorums(){
        return this.quorums;
    }
}
