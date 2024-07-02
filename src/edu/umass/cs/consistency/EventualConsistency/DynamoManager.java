package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.consistency.EventualConsistency.Domain.DAG;
import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;
import edu.umass.cs.consistency.EventualConsistency.Domain.RequestInformation;
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
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.umass.cs.consistency.EventualConsistency.Domain.DAG.addChildNode;
import static edu.umass.cs.consistency.EventualConsistency.Domain.DAG.createDominantChildGraphNode;

public class DynamoManager<NodeIDType> {
    private final PaxosMessenger<NodeIDType> messenger; // messaging
    private final int myID;
    private final Reconcilable myApp;
    private final HashMap<String, ReplicatedQuorumStateMachine> replicatedQuorums;
    // maps the quorumID to DAG associated
    private HashMap<String, DAG> requestDAG = new HashMap<>();
    private final Stringifiable<NodeIDType> unstringer;
    // a map of NodeIDType objects to integers
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    private Timestamp lastWriteTS = new Timestamp(0);
    //    Maps the reqestID to QuorumRequestAndCallback object
    private HashMap<Long, DynamoRequestAndCallback> requestsReceived = new HashMap<Long, DynamoRequestAndCallback>();
    private ArrayList<String> quorums = new ArrayList<String>();
    private final LargeCheckpointer largeCheckpointer;
    private static final Logger log = Logger.getLogger(DynamoManager.class.getName());

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
        this.myApp = (Reconcilable) LargeCheckpointer.wrap((Reconcilable) instance, largeCheckpointer);

        this.replicatedQuorums = new HashMap<>();

        this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));


        log.addHandler(new ConsoleHandler());
    }

    public static class DynamoRequestAndCallback {
        protected DynamoRequestPacket dynamoRequestPacket;
        final ExecutedCallback callback;
        protected Integer numOfAcksReceived;
        private ArrayList<GraphNode> responseGraphNodes;
        private HashMap<Long, String> responseRequests;

        DynamoRequestAndCallback(DynamoRequestPacket dynamoRequestPacket, ExecutedCallback callback) {
            this.dynamoRequestPacket = dynamoRequestPacket;
            this.callback = callback;
            this.responseGraphNodes = new ArrayList<>();
            this.responseRequests = new HashMap<>();
            this.numOfAcksReceived = 0;
        }

        @Override
        public String toString() {
            return this.dynamoRequestPacket + " [" + callback + "]";
        }

        public void reset() {
            this.numOfAcksReceived = 0;
        }

        public void setResponse(HashMap<Integer, Integer> vectorClock, String value, Timestamp ts) {
            dynamoRequestPacket.setTimestamp(ts);
            dynamoRequestPacket.setResponsePacket(new DynamoRequestPacket.DynamoPacket(vectorClock, value));
        }

        public Integer incrementAck(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm) {
            this.numOfAcksReceived += 1;
            if (qp.getType() == DynamoRequestPacket.DynamoPacketType.GET_ACK) {
                System.out.println("GET ACK received----------"+qp.getSource());
                System.out.println(qp.getResponsePacket().getAllRequests());
                System.out.println(qp.getResponsePacket().getVectorClock());
                this.addToArrayListForReconcile(qp);
            }
            return this.numOfAcksReceived;
        }

        public void addToArrayListForReconcile(DynamoRequestPacket responsePacket) {
            responseGraphNodes.add(new GraphNode(responsePacket.getRequestValue(), responsePacket.getResponsePacket().getVectorClock()));
            responseRequests.putAll(responsePacket.getResponsePacket().getAllRequests());
        }

        public ArrayList<GraphNode> getResponseGraphNodes() {
            return responseGraphNodes;
        }

        public void setResponseGraphNodes(ArrayList<GraphNode> responseGraphNodes) {
            this.responseGraphNodes = responseGraphNodes;
        }

        public HashMap<Long, String> getResponseRequests() {
            return responseRequests;
        }

        public void setResponseRequests(HashMap<Long, String> responseRequests) {
            this.responseRequests = responseRequests;
        }

        public Integer getNumOfAcksReceived() {
            return this.numOfAcksReceived;
        }

        public DynamoRequestPacket getResponsePacket() {
//            System.out.println("Returning resp pckt");
            this.dynamoRequestPacket.setPacketType(DynamoRequestPacket.DynamoPacketType.RESPONSE);
            return this.dynamoRequestPacket;
        }
    }

    public Timestamp getLastWriteTS() {
        return this.lastWriteTS;
    }

    //    class
    private void handleDynamoPacket(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback) {

        DynamoRequestPacket.DynamoPacketType packetType = qp.getType();

        switch (packetType) {
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
            case PUT_ACK:
            case GET_ACK:
                // write_quorum_node -> node
                handleAck(qp, rqsm);
                break;
            case TEST_GET_VC:
                handleTestGetVC(qp, rqsm, callback);
                break;
            case TEST_GET_REQ:
                handleTestGetReq(qp, rqsm, callback);
                break;
            default:
                break;
        }
    }

    private void handleTestGetReq(DynamoRequestPacket qp,
                                 ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){
        System.out.println("TEST GET request for : "+qp.getRequestValue()+" vc "+qp.getTestRequestVectorClock());
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new DynamoRequestAndCallback(qp, callback));
        qp.setAllRequests(this.requestDAG.get(rqsm.getQuorumID()).getAllRequestsWithVectorClockAsDominant(new GraphNode(qp.getRequestValue(), qp.getTestRequestVectorClock())));
        sendResponse(qp.getRequestID());
    }
    private void handleTestGetVC(DynamoRequestPacket qp,
                                 ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback){
        System.out.println("TEST request for : "+qp.getRequestValue());
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new DynamoRequestAndCallback(qp, callback));
        qp.setTestResponse(this.requestDAG.get(rqsm.getQuorumID()).getAllVC(qp.getRequestValue()));
        sendResponse(qp.getRequestID());
    }

    private void handlePutRequest(DynamoRequestPacket qp,
                                  ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback) {
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new DynamoRequestAndCallback(qp, callback));
        GraphNode requestGraphNode = createDominantChildGraphNode(this.requestDAG.get(rqsm.getQuorumID()).latestNodesForGivenObject(qp.getRequestValue()), qp);
        HashMap<Long, String> allRequests = this.requestDAG.get(rqsm.getQuorumID()).getAllRequests(qp.getRequestValue());
        requestGraphNode.getVectorClock().put(myID, requestGraphNode.getVectorClock().get(myID)+1);
        Request request = getInterfaceRequest(this.myApp, qp.toString());
        this.myApp.execute(request, false);
        qp.setTestRequestVectorClock(requestGraphNode.getVectorClock());
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.PUT_FWD);
        qp.setAllRequests(allRequests);
        requestGraphNode.addRequest(qp);
        for (int i = 0; i < rqsm.getQuorumMembers().size(); i++) {
            if (rqsm.getQuorumMembers().get(i) != this.myID) {
                qp.setSource(this.myID);
                qp.setDestination(rqsm.getQuorumMembers().get(i));
                this.sendRequest(qp, qp.getDestination());
            }
        }
    }

    public void handlePutForward(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm) {
        GraphNode graphNode = new GraphNode(qp.getRequestValue(), qp.getTestRequestVectorClock());
        ArrayList<GraphNode> latestNodes = this.requestDAG.get(rqsm.getQuorumID()).latestNodesWithVectorClockAsDominant(graphNode, true);
        if(latestNodes != null){
            addChildNode(latestNodes, graphNode);
            HashMap<Long, String> executedRequests = this.requestDAG.get(rqsm.getQuorumID()).getAllRequests(qp.getRequestValue());
            qp.getAllRequests().keySet().removeAll(executedRequests.keySet());
            graphNode.setRequests(executeRequests(qp.getAllRequests(), qp));
            graphNode.addRequests(qp);
            Request request = getInterfaceRequest(this.myApp, qp.toString());
            this.myApp.execute(request, false);
            this.lastWriteTS = new Timestamp(System.currentTimeMillis());
            qp.setPacketType(DynamoRequestPacket.DynamoPacketType.PUT_ACK);
            int dest = qp.getDestination();
            qp.setDestination(qp.getSource());
            qp.setSource(dest);
            qp.setResponsePacket(new DynamoRequestPacket.DynamoPacket(qp.getTestRequestVectorClock(), "-1"));
            this.sendRequest(qp, qp.getDestination());
        }
    }

    private void handleGetRequest(DynamoRequestPacket qp,
                                  ReplicatedQuorumStateMachine rqsm, ExecutedCallback callback) {
        System.out.println("GET request for "+qp.getRequestValue()+" received by "+myID);
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new DynamoRequestAndCallback(qp, callback));
        qp.setTestRequestVectorClock(qp.getTestRequestVectorClock());
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.GET_FWD);
        for (int i = 0; i < rqsm.getQuorumMembers().size(); i++) {
            qp.setSource(this.myID);
            qp.setDestination(rqsm.getQuorumMembers().get(i));
            this.sendRequest(qp, qp.getDestination());
        }
    }

    public void handleGetForward(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm) {
        HashMap<Long, String> allRequests = this.requestDAG.get(rqsm.getQuorumID()).getAllRequests(qp.getRequestValue());
        GraphNode requestGraphNode = createDominantChildGraphNode(this.requestDAG.get(rqsm.getQuorumID()).latestNodesForGivenObject(qp.getRequestValue()), qp);
        requestGraphNode.getVectorClock().put(myID, requestGraphNode.getVectorClock().get(myID)+1);
        System.out.println(requestGraphNode);
        Request request = getInterfaceRequest(this.myApp, qp.toString());
        this.myApp.execute(request, false);
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.GET_ACK);
        qp.setTimestamp(this.lastWriteTS);
        int dest = qp.getDestination();
        qp.setDestination(qp.getSource());
        qp.setSource(dest);
        assert request != null;
        qp.setResponsePacket(new DynamoRequestPacket.DynamoPacket(requestGraphNode.getVectorClock(), ((DynamoRequestPacket) request).getResponsePacket().getValue()));
        qp.getResponsePacket().setAllRequests(allRequests);
        this.sendRequest(qp, qp.getDestination());
    }

    public void handleAck(DynamoRequestPacket qp, ReplicatedQuorumStateMachine rqsm) {
        if (qp.getType() == DynamoRequestPacket.DynamoPacketType.PUT_ACK) {
            try {
                if (this.requestsReceived.get(qp.getRequestID()).incrementAck(qp, rqsm) == rqsm.getWriteQuorum() - 1) {
                    this.requestsReceived.get(qp.getRequestID()).setResponse(qp.getTestRequestVectorClock(), qp.getResponsePacket().getValue(), qp.getTimestamp());
                    sendResponse(qp.getRequestID());
                }
            } catch (Exception e) {
                log.log(Level.WARNING, e.toString());
            }
        } else if (qp.getType() == DynamoRequestPacket.DynamoPacketType.GET_ACK) {
            try {
                if (this.requestsReceived.get(qp.getRequestID()).incrementAck(qp, rqsm) == rqsm.getReadQuorum()) {
                    ArrayList<GraphNode> responseGraphNodes = new ArrayList<>(this.requestsReceived.get(qp.getRequestID()).getResponseGraphNodes());
                    GraphNode reconciledResponse = this.myApp.reconcile(responseGraphNodes);
                    HashMap<Long, String> executedRequests = this.requestDAG.get(rqsm.getQuorumID()).getAllRequests(qp.getRequestValue());
                    HashMap<Long, String> obtainedRequests = this.requestsReceived.get(qp.getRequestID()).getResponseRequests();
                    obtainedRequests.keySet().removeAll(executedRequests.keySet());
                    reconciledResponse.setRequests(executeRequests(obtainedRequests, qp));
                    addChildNode(this.requestDAG.get(rqsm.getQuorumID()).latestNodesWithVectorClockAsDominant(reconciledResponse, false), reconciledResponse);
                    Request request = getInterfaceRequest(this.myApp, qp.toString());
                    this.myApp.execute(request, false);
                    this.requestsReceived.get(qp.getRequestID()).setResponse(reconciledResponse.getVectorClock(), qp.getResponsePacket().getValue(), qp.getTimestamp());
                    sendResponse(qp.getRequestID());
                }
            } catch (Exception e) {
                log.log(Level.WARNING, e.toString());
            }
        }
    }
    private ArrayList<RequestInformation> executeRequests(HashMap<Long, String> hashMap, DynamoRequestPacket dynamoRequestPacket){
        ArrayList<RequestInformation> requestInformationArrayList = new ArrayList<>();
        DynamoRequestPacket dummyRequestPacket = new DynamoRequestPacket(dynamoRequestPacket.getRequestID(), DynamoRequestPacket.DynamoPacketType.GET, dynamoRequestPacket);
        for(Long key: hashMap.keySet()){
            String[] strings = hashMap.get(key).split(" ");
            dummyRequestPacket.setPacketType(DynamoRequestPacket.DynamoPacketType.getDynamoPacketType(strings[0]));
            dummyRequestPacket.setRequestValue(strings[1]);
            Request request = getInterfaceRequest(this.myApp, dummyRequestPacket.toString());
            this.myApp.execute(request, false);
            requestInformationArrayList.add(new RequestInformation(key, hashMap.get(key)));
        }
        return requestInformationArrayList;
    }
    public void sendResponse(Long requestID) {
        DynamoRequestAndCallback requestAndCallback = this.requestsReceived.get(requestID);
        if (requestAndCallback != null && requestAndCallback.callback != null) {

            requestAndCallback.callback.executed(requestAndCallback.getResponsePacket()
                    , true);
            this.requestsReceived.remove(requestID);

        } else {
            log.log(Level.WARNING, "QuorumManager.handleResponse received " +
                            "an ACK request {0} that does not match any enqueued request.",
                    new Object[]{requestID});
        }
    }

    private void sendRequest(DynamoRequestPacket qp,
                             int nodeID) {
        GenericMessagingTask<NodeIDType, ?> gTask = null;
        try {
            // forward to nodeID
            gTask = new GenericMessagingTask(this.integerMap.get(nodeID),
                    qp);
            this.messenger.send(gTask);
        } catch (Exception e) {
            e.printStackTrace();
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

    private DynamoRequestPacket getDynamoRequestPacket(Request request) {
        try {
            return (DynamoRequestPacket) request;
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        return null;
    }

    private ReplicatedQuorumStateMachine getInstance(String quorumID) {
        return this.replicatedQuorums.get(quorumID);
    }

    public boolean createReplicatedQuorumForcibly(String quorumID, int version,
                                                  Set<NodeIDType> nodes, Replicable app,
                                                  String state) {
        return this.createReplicatedQuorumFinal(quorumID, version, nodes, app, state) != null;
    }

    private synchronized ReplicatedQuorumStateMachine createReplicatedQuorumFinal(
            String quorumID, int version, Set<NodeIDType> nodes,
            Replicable app, String initialState) {
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm != null)
            return rqsm;
        try {
            rqsm = new ReplicatedQuorumStateMachine(quorumID, version, myID,
                    this.integerMap.put(nodes), app != null ? app : this.myApp,
                    initialState, this);
            quorums.add(quorumID);
            System.out.println("Creating new Replicated Quorum State Machine: " + rqsm);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        this.putInstance(quorumID, rqsm);
        this.integerMap.put(nodes);
        this.initializeDAG(quorumID, rqsm);
        return rqsm;
    }

    public Set<NodeIDType> getReplicaGroup(String quorumID) {
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm == null)
            return null;
        return this.integerMap.getIntArrayAsNodeSet(rqsm.getQuorumMembersArray());
    }

    public boolean deleteReplicatedQuorum(String quorumID, int epoch) {
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm == null)
            return true;
        if (rqsm.getVersion() > epoch) {
            return false;
        }
        return this.removeInstance(quorumID);
    }

    private boolean removeInstance(String quorumID) {
        return this.replicatedQuorums.remove(quorumID) != null;
    }

    private void putInstance(String quorumID, ReplicatedQuorumStateMachine rqsm) {
        this.replicatedQuorums.put(quorumID, rqsm);
    }

    private void initializeDAG(String quorumID, ReplicatedQuorumStateMachine rqsm) {
        HashMap<String, HashMap<Integer, Integer>> vectorClock = new HashMap<>();
        vectorClock.put(quorumID, new HashMap<Integer, Integer>());
        for (int i = 0; i < rqsm.getQuorumMembers().size(); i++) {
            vectorClock.get(quorumID).put(rqsm.getQuorumMembers().get(i), 0);
        }
        this.requestDAG.put(quorumID, new DAG(vectorClock.get(quorumID)));
    }

    public Integer getVersion(String quorumID) {
        ReplicatedQuorumStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm != null)
            return (int) rqsm.getVersion();
        return -1;
    }
}
