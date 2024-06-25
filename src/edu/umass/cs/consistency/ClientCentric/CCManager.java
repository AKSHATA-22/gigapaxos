package edu.umass.cs.consistency.ClientCentric;

import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
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
import org.json.JSONException;
import org.json.JSONObject;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CCManager<NodeIDType> {
    private final PaxosMessenger<NodeIDType> messenger;
    private final int myID;
    private final Replicable myApp;
    private final FailureDetection<NodeIDType> FD;
    private final HashMap<String, CCReplicatedStateMachine> replicatedSM = new HashMap<>();
    private final HashMap<String, ArrayList<Write>> writesByServer = new HashMap<>();
    private final HashMap<String, HashMap<Integer, Timestamp>> wvc = new HashMap<>();
    private HashMap<Long, MRRequestAndCallback> requestsReceived = new HashMap<Long, MRRequestAndCallback>();
    private ArrayList<String> serviceNames = new ArrayList<String>();
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    public final Stringifiable<NodeIDType> unstringer;
    private final LargeCheckpointer largeCheckpointer;
    private static final Logger log = Logger.getLogger(ReconfigurationConfig.class.getName());
    public static final Class<?> application = CCApp.class;
    public CCManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
                     InterfaceNIOTransport<NodeIDType, JSONObject> niot, Replicable instance,
                     String logFolder, boolean enableNullCheckpoints) {
        this.myID = this.integerMap.put(id);

        this.unstringer = unstringer;

        this.largeCheckpointer = new LargeCheckpointer(logFolder,
                id.toString());

        this.myApp = LargeCheckpointer.wrap(instance, largeCheckpointer);

        this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));
        this.FD = new FailureDetection<>(id, niot, null);

    }
    /**
    This class is used to represent a write.
     **/
    public static class Write{
        private String statement;
        private Timestamp ts;
        private int node;
        Write(Timestamp ts, String statement, int node){
            this.statement = statement;
            this.ts = ts;
            this.node = node;
        }
//        write a toString for this class
        @Override
        public String toString(){
            try {
                return this.toJSONObjectImpl().toString();
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }

        protected JSONObject toJSONObjectImpl() throws JSONException {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("ts", this.ts);
            jsonObject.put("statement", this.statement);
            jsonObject.put("node", this.node);
            return jsonObject;
        }
        public String getStatement(){
            return this.statement;
        }

        public Timestamp getTs() {
            return ts;
        }

        public int getNode() {
            return node;
        }
    }

    /**
     * Comparator for the Priority queue that consists of objects of type Write
     */
    class WriteComparator implements Comparator<Write>{
        public int compare(Write w1, Write w2) {
            int compare = w1.ts.compareTo(w2.ts);
            if (compare > 0)
                return 1;
            else if (compare < 0)
                return -1;
            return 0;
        }
    }

    /**
     * Maps the request to the appropriate callback.
     * Fields:
     * 1. mrRequestPacket - Original request packet
     * 2. callback - Associated callback
     * 3. requestSent - Set of nodes to which a request is sent
     * 4. pq - priority queue which will contain all the response writes ordered by their timestamp
     */
    public class MRRequestAndCallback {
        protected CCRequestPacket CCRequestPacket;
        final ExecutedCallback callback;
        protected Set<Integer> requestSent = new HashSet<>();
        PriorityQueue<Write> pq = new PriorityQueue<Write>(new WriteComparator());

        MRRequestAndCallback(CCRequestPacket CCRequestPacket, ExecutedCallback callback){
            this.CCRequestPacket = new CCRequestPacket(CCRequestPacket.getRequestID(), CCRequestPacket.getPacketType(), CCRequestPacket);
            this.callback = callback;
        }

        @Override
        public String toString(){
            return this.CCRequestPacket +" ["+ callback+"]";
        }
//        Once an ack is received the response writes are added to the priority queue. Return true if the requestSent set is empty.
        public boolean ackReceived(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm){
            removeReqFromSet(CCRequestPacket.getSource());
            if(!CCRequestPacket.getResponseWrites().isEmpty()) {
                this.addWrites(CCRequestPacket.getResponseWrites().get(CCRequestPacket.getSource()));
            }
            if (this.requestSent.isEmpty()){
                return true;
            }
            return false;
        }
        public boolean isWrite(){
//            System.out.println("isWrite: "+this.CCRequestPacket.getPacketType()+(this.CCRequestPacket.getPacketType() == edu.umass.cs.consistency.MonotonicReads.CCRequestPacket.CCPacketType.WRITE));
            return this.CCRequestPacket.getPacketType() == edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.MR_WRITE || this.CCRequestPacket.getPacketType() == edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.MW_WRITE;
        }
        public void addCurrentIfNeeded(Integer nodeID, Timestamp ts){
            if(this.CCRequestPacket.getPacketType() == edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.MW_WRITE || this.CCRequestPacket.getPacketType() == edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.MR_WRITE){
                this.CCRequestPacket.addResponseWrites(nodeID, ts, this.CCRequestPacket.getRequestValue());
            }
        }
        public void setResponse(HashMap<Integer, Timestamp> responseVectorClock, String responseValue){
            this.CCRequestPacket.setPacketType(edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.RESPONSE);
            this.CCRequestPacket.setResponseVectorClock(responseVectorClock);
            this.CCRequestPacket.setResponseValue(responseValue);
        }
        public void removeReqFromSet(Integer node){
            this.requestSent.remove(node);
        }
        public void addReqToSet(Integer node){
            this.requestSent.add(node);
        }
        public PriorityQueue<Write> getPq() {
            return this.pq;
        }
        public void addWrites(ArrayList<Write> arrayListToAdd){
            this.pq.addAll(arrayListToAdd);
        }
        public CCRequestPacket getMrRequestPacket() {
            return this.CCRequestPacket;
        }

        public Integer getRequestSentLength() {
            System.out.println(this.requestSent);
            return requestSent.size();
        }
    }
    private void handlePacket(CCRequestPacket qp, CCReplicatedStateMachine mrsm, ExecutedCallback callback){

        CCRequestPacket.CCPacketType packetType = qp.getType();

        switch(packetType) {
            case MR_READ:
                handleReadRequestMR(qp, mrsm, callback);
                break;
            case MR_WRITE:
                handleWriteRequest(qp, mrsm, callback);
                break;
            case MW_WRITE:
                handleWriteRequest(qp, mrsm, callback);
                break;
            case MW_READ:
                handleReadRequestMW(qp, mrsm, callback);
                break;
            case FWD:
                handleFwdRequest(qp, mrsm);
                break;
            case FWD_ACK:
                handleFwdAck(qp, mrsm);
                break;
            case FAILURE_DETECT:
                processFailureDetection(qp);
                break;
            default:
                break;
        }

    }

    /**
     * If the vector clock of the request is greater gets the writes in that time, else handles the request as an ack
     * @param CCRequestPacket
     * @param mrsm
     * @param callback
     */
    private void handleReadRequestMR(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm, ExecutedCallback callback){
        System.out.println("Read received: ---"+ CCRequestPacket);
        this.requestsReceived.putIfAbsent(CCRequestPacket.getRequestID(), new MRRequestAndCallback(CCRequestPacket, callback));
        if(!CCRequestPacket.getRequestVectorClock().isEmpty()){
            CCRequestPacket.setPacketType(edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.FWD);
            for (int i = 0; i < mrsm.getMembers().size(); i++) {
                if (mrsm.getMembers().get(i) != this.myID & CCRequestPacket.getRequestVectorClock().get(mrsm.getMembers().get(i)).compareTo(this.wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i))) > 0) {
                    if (!isNodeUp(mrsm.getMembers().get(i))){
                        this.requestsReceived.get(CCRequestPacket.getRequestID()).addWrites(CCRequestPacket.getRequestWrites().get(mrsm.getMembers().get(i)));
                    }
                    else {
                        CCRequestPacket.setWritesFrom(this.wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i)));
                        CCRequestPacket.setWritesTo(CCRequestPacket.getRequestVectorClock().get(mrsm.getMembers().get(i)));
                        CCRequestPacket.setSource(this.myID);
                        CCRequestPacket.setDestination(mrsm.getMembers().get(i));
                        this.requestsReceived.get(CCRequestPacket.getRequestID()).addReqToSet(CCRequestPacket.getDestination());
                        this.sendRequest(CCRequestPacket, CCRequestPacket.getDestination());
                    }
                }
            }
        }
        if(CCRequestPacket.getRequestVectorClock().isEmpty() || this.requestsReceived.get(CCRequestPacket.getRequestID()).getRequestSentLength() == 0) {
            System.out.println("Request vector clock is empty");
            CCRequestPacket.setResponseVectorClock(this.wvc.get(mrsm.getServiceName()));
            CCRequestPacket.setPacketType(edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.FWD_ACK);
            CCRequestPacket.setSource(this.myID);
            this.requestsReceived.get(CCRequestPacket.getRequestID()).addReqToSet(CCRequestPacket.getSource());
            handlePacket(CCRequestPacket, mrsm, callback);
        }
    }

    /**
     * For monotonic writes the read request is directly executed
     * @param CCRequestPacket
     * @param mrsm
     * @param callback
     */
    private void handleReadRequestMW(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm, ExecutedCallback callback){
        this.requestsReceived.putIfAbsent(CCRequestPacket.getRequestID(), new MRRequestAndCallback(CCRequestPacket, callback));
        CCRequestPacket.setResponseVectorClock(this.wvc.get(mrsm.getServiceName()));
        CCRequestPacket.setPacketType(edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.FWD_ACK);
        CCRequestPacket.setSource(this.myID);
        this.requestsReceived.get(CCRequestPacket.getRequestID()).addReqToSet(CCRequestPacket.getSource());
        handlePacket(CCRequestPacket, mrsm, callback);
    }

    /**
     * If the vector clock of the request is greater gets all the writes after it, else handles the request as an ack
     * @param CCRequestPacket
     * @param mrsm
     * @param callback
     */
    private void handleWriteRequest(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm, ExecutedCallback callback){
        System.out.println("Write received:---"+ CCRequestPacket);
        this.requestsReceived.putIfAbsent(CCRequestPacket.getRequestID(), new MRRequestAndCallback(CCRequestPacket, callback));
        if(!CCRequestPacket.getRequestVectorClock().isEmpty()) {
            CCRequestPacket.setPacketType(edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.FWD);
            for (int i = 0; i < mrsm.getMembers().size(); i++) {
                if (CCRequestPacket.getRequestVectorClock().get(mrsm.getMembers().get(i)).compareTo(this.wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i))) > 0) {
                    if (!isNodeUp(mrsm.getMembers().get(i))){
                        this.requestsReceived.get(CCRequestPacket.getRequestID()).addWrites(CCRequestPacket.getRequestWrites().get(mrsm.getMembers().get(i)));
                    }
                    else {
                        CCRequestPacket.setWritesFrom(this.wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i)));
                        CCRequestPacket.setSource(this.myID);
                        CCRequestPacket.setDestination(mrsm.getMembers().get(i));
                        this.requestsReceived.get(CCRequestPacket.getRequestID()).addReqToSet(CCRequestPacket.getDestination());
                        this.sendRequest(new CCRequestPacket(CCRequestPacket.getRequestID(), CCRequestPacket.getPacketType(), CCRequestPacket), CCRequestPacket.getDestination());
                    }
                }
            }
        }
        if (CCRequestPacket.getRequestVectorClock().isEmpty() || this.requestsReceived.get(CCRequestPacket.getRequestID()).getRequestSentLength() == 0) {
                CCRequestPacket.setResponseVectorClock(this.wvc.get(mrsm.getServiceName()));
                CCRequestPacket.setPacketType(edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.FWD_ACK);
                CCRequestPacket.setSource(this.myID);
                this.requestsReceived.get(CCRequestPacket.getRequestID()).addReqToSet(CCRequestPacket.getSource());
                this.handlePacket(CCRequestPacket, mrsm, callback);
        }

    }

    /**
     * Adds writes from own write set to the response
     * @param CCRequestPacket
     * @param mrsm
     */
    private void handleFwdRequest(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm){
        this.heardFrom(CCRequestPacket.getSource());
        CCRequestPacket.setPacketType(edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.FWD_ACK);
        if (CCRequestPacket.getWritesTo().equals(new Timestamp(0))){
            CCRequestPacket.setWritesTo(new Timestamp(System.currentTimeMillis()));
        }
        System.out.println("handle forward received:---"+ CCRequestPacket);
        for (Write write: this.writesByServer.get(mrsm.getServiceName())){
            System.out.println("------------------------------------"+write);
            if((write.getTs().compareTo(CCRequestPacket.getWritesFrom()) >= 0) & (write.getTs().compareTo(CCRequestPacket.getWritesTo()) <= 0)){
                System.out.println("Adding to response writes: "+write.getStatement());
                CCRequestPacket.addResponseWrites(this.myID, write.getTs(), write.getStatement());
            }
        }
        int dest = CCRequestPacket.getDestination();
        CCRequestPacket.setDestination(CCRequestPacket.getSource());
        CCRequestPacket.setSource(dest);
        this.sendRequest(CCRequestPacket, CCRequestPacket.getDestination());
    }

    /**
     * If writes from all nodes are received, executes the writes in the priority queue. Updates vector clock and write set if needed
     * @param CCRequestPacket
     * @param mrsm
     */
    private void handleFwdAck(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm){
        this.heardFrom(CCRequestPacket.getSource());
        System.out.println("handle forward ack received:--"+ CCRequestPacket);
        if(this.requestsReceived.get(CCRequestPacket.getRequestID()).ackReceived(CCRequestPacket, mrsm)){
            for (Write write : this.requestsReceived.get(CCRequestPacket.getRequestID()).getPq()){
                CCRequestPacket.setPacketType(edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.MW_WRITE);
                CCRequestPacket.setRequestValue(write.getStatement());
                this.wvc.get(mrsm.getServiceName()).put(write.getNode(), write.getTs());
                Request request = getInterfaceRequest(this.myApp, CCRequestPacket.toString());
                this.myApp.execute(request, false);
            }
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            System.out.println("after execution: "+this.wvc.get(mrsm.getServiceName()));
            this.requestsReceived.get(CCRequestPacket.getRequestID()).addCurrentIfNeeded(this.myID, ts);
            Request request = getInterfaceRequest(this.myApp, this.requestsReceived.get(CCRequestPacket.getRequestID()).getMrRequestPacket().toString());
            this.myApp.execute(request, false);
            assert request != null;
            if (this.requestsReceived.get(CCRequestPacket.getRequestID()).isWrite()) {
                if(((CCRequestPacket)request).getResponseValue().equals("EXECUTED")) {
                    this.wvc.get(mrsm.getServiceName()).put(this.myID, ts);
                    this.writesByServer.get(mrsm.getServiceName()).add(new Write(ts, this.requestsReceived.get(CCRequestPacket.getRequestID()).getMrRequestPacket().getRequestValue(), this.myID));
                }
            }
            this.requestsReceived.get(CCRequestPacket.getRequestID()).setResponse(this.wvc.get(mrsm.getServiceName()), ((CCRequestPacket)request).getResponseValue());
            sendResponse(CCRequestPacket.getRequestID(), this.requestsReceived.get(CCRequestPacket.getRequestID()).getMrRequestPacket());
        }
    }
    private void processFailureDetection(CCRequestPacket request) {
        try {
            FailureDetectionPacket<NodeIDType> failureDetectionPacket = new FailureDetectionPacket<>(new JSONObject(request.getRequestValue()), this.unstringer);
            FD.receive(failureDetectionPacket);
        }
        catch (Exception e){
            System.out.println("Exception: "+e);
        }
    }
    public void sendResponse(Long requestID, CCRequestPacket mrResponsePacket){
        MRRequestAndCallback requestAndCallback = this.requestsReceived.get(requestID);
        if (requestAndCallback != null && requestAndCallback.callback != null) {
            mrResponsePacket.setPacketType(CCRequestPacket.CCPacketType.RESPONSE);
            mrResponsePacket.setSource(this.myID);
//            System.out.println("Sending resp packet: "+mrResponsePacket);
            requestAndCallback.callback.executed(mrResponsePacket
                    , true);
            this.requestsReceived.remove(requestID);
        } else {
            // can't find the request being queued in outstanding
            log.log(Level.WARNING, "QuorumManager.handleResponse received " +
                            "an ACK request {0} that does not match any enqueued request.",
                    new Object[]{requestID});
        }
    }
    private void sendRequest(CCRequestPacket CCRequestPacket,
                             int nodeID){
        GenericMessagingTask<NodeIDType,?> gTask = null;
        System.out.println("sending req:--"+ CCRequestPacket +" to node "+nodeID);
        try {
            // forward to nodeID
            gTask = new GenericMessagingTask(this.integerMap.get(nodeID),
                    CCRequestPacket);
            this.messenger.send(gTask);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static Request getInterfaceRequest(Replicable app, String value) {
        try {
//            System.out.println("VALUE--------"+value);
            return app.getRequest(value);
        } catch (RequestParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    public boolean createReplicatedQuorumForcibly(String serviceName, int version,
                                                  Set<NodeIDType> nodes, Replicable app,
                                                  String state){
        return this.createReplicatedQuorumFinal(serviceName, version, nodes, app, state) != null;
    }
    private synchronized CCReplicatedStateMachine createReplicatedQuorumFinal(
            String serviceName, int version, Set<NodeIDType> nodes,
            Replicable app, String initialState){
        CCReplicatedStateMachine mrrsm = this.getInstance(serviceName);
        if (mrrsm != null)
            return mrrsm;
        try {
            mrrsm = new CCReplicatedStateMachine(serviceName, version, myID,
                    this.integerMap.put(nodes), app != null ? app : this.myApp,
                    initialState, this);
            serviceNames.add(serviceName);
            System.out.println("Creating new Replicated Monotonic Reads State Machine: "+ mrrsm);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        this.putInstance(serviceName, mrrsm);
        this.integerMap.put(nodes);
        this.FD.sendKeepAlive(nodes);
        this.putVectorClock(serviceName, mrrsm);
        this.initializeWriteSet(serviceName);
        return mrrsm;
    }
    public boolean deleteReplicatedQuorum(String serviceName, int epoch){
        CCReplicatedStateMachine mrsm = this.getInstance(serviceName);
        if(mrsm == null)
            return true;
        if(mrsm.getVersion() > epoch) {
            return false;
        }
        return this.removeInstance(serviceName);
    }
    public String propose(String serviceName, Request request,
                          ExecutedCallback callback) {
        System.out.println(request+"");
        if(request.getRequestType() == CCRequestPacket.CCPacketType.FAILURE_DETECT){
            this.handlePacket((CCRequestPacket)request, null, callback);
            return null;
        }
        CCRequestPacket CCRequestPacket = this.getMRRequestPacket(request);

        boolean matched = false;

        CCReplicatedStateMachine mrsm = this.getInstance(serviceName);

        if (mrsm != null) {
            matched = true;
            assert CCRequestPacket != null;
            CCRequestPacket.setServiceName(serviceName);
            this.handlePacket(CCRequestPacket, mrsm, callback);
        } else {
            System.out.println("The given quorumID "+serviceName+" has no state machine associated");
        }


        return matched ? mrsm.getServiceName() : null;
    }
    private CCRequestPacket getMRRequestPacket(Request request){
        try {
            return (CCRequestPacket) request;
        }
        catch (Exception e){
            System.out.println(e.toString());
        }
        return null;
    }
    private CCReplicatedStateMachine getInstance(String quorumID){
        return this.replicatedSM.get(quorumID);
    }
    private void putInstance(String quorumID, CCReplicatedStateMachine mrrsm){
        this.replicatedSM.put(quorumID, mrrsm);
    }
    private void putVectorClock(String serviceName, CCReplicatedStateMachine mrrsm){
        this.wvc.put(serviceName, new HashMap<Integer, Timestamp>());
        for (int i = 0; i < mrrsm.getMembers().size(); i++) {
            this.wvc.get(serviceName).put(mrrsm.getMembers().get(i), new Timestamp(0));
        }
        System.out.println("wvc initialized: "+ this.wvc);
    }
    public void initializeWriteSet(String serviceName){
        this.writesByServer.put(serviceName, new ArrayList<>());
    }
    private boolean removeInstance(String serviceName) {
        return this.replicatedSM.remove(serviceName) != null;
    }
    public Integer getVersion(String quorumID) {
        CCReplicatedStateMachine mrsm = this.getInstance(quorumID);
        if ( mrsm != null)
            return (int) mrsm.getVersion();
        return -1;
    }
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        CCReplicatedStateMachine mrsm = this.getInstance(serviceName);
        if (mrsm == null)
            return null;
        return this.integerMap.getIntArrayAsNodeSet(mrsm.getMembersArray());
    }
    public static final String getDefaultServiceName() {
        return application.getSimpleName() + "0";
    }
    protected void heardFrom(int id) {
        System.out.println(this.myID+" heard from: "+id);
        try {
            this.FD.heardFrom(this.integerMap.get(id));
        } catch (RuntimeException re) {
            // do nothing, can happen during recovery
            System.out.println(re.toString());
        }
    }
    protected boolean isNodeUp(int id) {
        return (FD != null ? FD.isNodeUp(this.integerMap.get(id)) : false);
    }

    protected long getDeadTime(int id) {
        return (FD != null ? FD.getDeadTime(this.integerMap.get(id)) : System
                .currentTimeMillis());
    }
}
