package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.consistency.Quorum.ReplicatedQuorumStateMachine;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Reconcilable;
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
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DynamoManager<NodeIDType> {
    private final PaxosMessenger<NodeIDType> messenger; // messaging
    private final int myID;
    private final Reconcilable myApp;
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
    private final ScheduledExecutorService execpool;
    private HashMap<Integer, ScheduledFuture<ReconcileTask>> futures;
    private ArrayList<StatusReportPacket> receivedStatusReports;
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
        this.myApp = (Reconcilable) LargeCheckpointer.wrap((Reconcilable)instance, largeCheckpointer);

        this.replicatedQuorums = new HashMap<>();

        this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));

        this.execpool = Executors.newScheduledThreadPool(1,
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = Executors.defaultThreadFactory()
                                .newThread(r);
                        thread.setName(edu.umass.cs.consistency.EventualConsistency.StatusReport.class.getSimpleName()
                                + myID);
                        return thread;
                    }
                });
    }

    public static class DynamoRequestAndCallback {
        protected DynamoRequestPacket dynamoRequestPacket;
        final ExecutedCallback callback;
        protected Integer numOfAcksReceived = 0;
        private ArrayList<DynamoRequestPacket> responsesReceived;

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
        public void setResponse(HashMap<Integer, Integer> vectorClock, String value,Timestamp ts){
            dynamoRequestPacket.setTimestamp(ts);
            dynamoRequestPacket.setResponsePacket(new DynamoRequestPacket.DynamoPacket(vectorClock, value));
        }
        public Integer incrementAck(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm){
            this.numOfAcksReceived += 1;
            if (qp.getType() == DynamoRequestPacket.DynamoPacketType.GET_ACK){
                System.out.println("GET ACK received----------");
                this.addToArrayListForReconcile(qp);
            }
            return this.numOfAcksReceived;
        }
        public void addToArrayListForReconcile(DynamoRequestPacket responsePacket){
            this.responsesReceived.add(responsePacket);
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
    class ReconcileTask implements Runnable{
//        private StatusReportPacket statusReportPacket;
        private final Reconcilable myApp;
        private final int id;
        private final int source;
        private final String quorumID;
        ReconcileTask(int source, int id, Reconcilable myApp, String quorumID){
            this.source = source;
            this.id = id;
            this.myApp = myApp;
            this.quorumID = quorumID;
        }
        public void run(){
            log.log(Level.INFO, "Sending Status report to: ", this.id);
            StatusReportPacket statusReportPacket = getStatusReportPacket(source, this.id, this.quorumID);
            sendStatusReport(statusReportPacket);
            System.out.println("Sent Status report to: "+ this.id);
        }
    }
    public StatusReportPacket getStatusReportPacket(int source, int destination, String quorumID){
        StatusReportPacket statusReportPacket = new StatusReportPacket((long) (Math.random() * Integer.MAX_VALUE),
                new DynamoRequestPacket.DynamoPacket(getVectorClock(quorumID), this.myApp.stateForReconcile()),
                getLastWriteTS());
        statusReportPacket.setQuorumID(quorumID);
        statusReportPacket.setSource(source);
        statusReportPacket.setDestination(destination);
        return statusReportPacket;
    }
    private void startStatusReport(String quorumID){
        System.out.println("QuorumID: "+quorumID);
        System.out.println("Replicated Quorums: "+this.replicatedQuorums);
        ArrayList<Integer> membersList = this.replicatedQuorums.get(quorumID).getQuorumMembers();
        System.out.println("Members: "+membersList);
        for (Integer member: membersList){
            try {
                ReconcileTask reconcileTask = new ReconcileTask(this.myID, member, this.myApp, quorumID);
                System.out.println("Sending status report");
//                reconcileTask.run(); // run once immediately
                ScheduledFuture<?> future = execpool
                        .scheduleAtFixedRate(reconcileTask,
                                10,
                                1000,
                                TimeUnit.MILLISECONDS);
                futures.put(
                        member,
                        (ScheduledFuture<ReconcileTask>) future);

            } catch (Exception e) {
                log.severe("Can not create ping packet at node " + this.myID
                        + " for node " + member);
                e.printStackTrace();
            }
        }
    }
    public HashMap<Integer, Integer> getVectorClock(String quorumID){
        return this.vectorClock.get(quorumID);
    }
    public Timestamp getLastWriteTS(){
        return this.lastWriteTS;
    }
//    class
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
    private void handleStatusReport(StatusReportPacket qp){
        log.log(Level.INFO, "{0} Received status report packet from {1}", new Object[]{this.myID, qp.getSource()});
        System.out.println("Received status report packet"+qp.toString());
        StatusReportPacket statusReportPacket = getStatusReportPacket(this.myID, this.myID, qp.getQuorumID());
        ArrayList<Request> arrayList = new ArrayList<>();
        arrayList.add(statusReportPacket);
        arrayList.add(qp);
        StatusReportPacket statusReportPacketResponse = (StatusReportPacket) this.myApp.reconcile(arrayList);
//        System.out.println("---------------------"+statusReportPacketResponse);
        if(statusReportPacketResponse.getSource() != statusReportPacketResponse.getDestination())
            this.vectorClock.put(qp.getQuorumID(), statusReportPacketResponse.getRequestPacket().getVectorClock());
    }
    private void testCodeToGetState(DynamoRequestPacket qp){
        System.out.println("=================================here===========================");
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.GET_FWD);
        Request request = getInterfaceRequest(this.myApp, qp.toString());
        this.myApp.execute(request, false);
        assert request != null;
//        System.out.println("value"+((DynamoRequestPacket)request).getResponsePacket().getValue());
        this.requestsReceived.get(qp.getRequestID()).setResponse(null,((DynamoRequestPacket)request).getResponsePacket().getValue(), null);
        sendResponse(qp.getRequestID());
    }
    private void handleGetRequest(DynamoRequestPacket qp,
                                  ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new DynamoRequestAndCallback(qp, callback));
        System.out.println("REQUEST VALUE-->"+qp.getRequestValue());
        if(qp.getRequestValue().isEmpty()){
            testCodeToGetState(qp);
        }
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
        qp.setResponsePacket(new DynamoRequestPacket.DynamoPacket(this.vectorClock.get(rqsm.getQuorumID()), "-1"));
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
                    ArrayList<Request> arrayList = new ArrayList<>(this.requestsReceived.get(qp.getRequestID()).responsesReceived);
                    DynamoRequestPacket reconciledResponse = (DynamoRequestPacket) this.myApp.reconcile(arrayList);
                    this.requestsReceived.get(qp.getRequestID()).setResponse(reconciledResponse.getResponsePacket().getVectorClock(), reconciledResponse.getResponsePacket().getValue(), reconciledResponse.getTimestamp());
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
            System.out.println("RESPONSE: "+requestAndCallback.getResponsePacket());
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
    private void sendStatusReport(StatusReportPacket qp){
        GenericMessagingTask<NodeIDType,?> gTask = null;
        try {
            // forward to nodeID
            gTask = new GenericMessagingTask(this.integerMap.get(qp.getDestination()),
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

        if(request.getRequestType() == DynamoRequestPacket.DynamoPacketType.STATUS_REPORT){
            handleStatusReport((StatusReportPacket) request);
            return null;
        }
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
        startStatusReport(quorumID);
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
