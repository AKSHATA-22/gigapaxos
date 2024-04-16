package edu.umass.cs.consistency.MonotonicReads;

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

public class MRManager<NodeIDType> {
    private final PaxosMessenger<NodeIDType> messenger;
    private final int myID;
    private final Replicable myApp;
    private final FailureDetection<NodeIDType> FD;
    private final HashMap<String, MRReplicatedStateMachine> replicatedSM = new HashMap<>();
    private final HashMap<String, ArrayList<Write>> writesByServer = new HashMap<>();
    private final HashMap<String, HashMap<Integer, Timestamp>> wvc = new HashMap<>();
    private HashMap<Long, MRRequestAndCallback> requestsReceived = new HashMap<Long, MRRequestAndCallback>();
    private ArrayList<String> serviceNames = new ArrayList<String>();
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    public final Stringifiable<NodeIDType> unstringer;
    private final LargeCheckpointer largeCheckpointer;
    private static final Logger log = Logger.getLogger(ReconfigurationConfig.class.getName());
    public static final Class<?> application = MRApp.class;
    public MRManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
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
        protected MRRequestPacket mrRequestPacket;
        final ExecutedCallback callback;
        protected Set<Integer> requestSent = new HashSet<>();
        PriorityQueue<Write> pq = new PriorityQueue<Write>(new WriteComparator());

        MRRequestAndCallback(MRRequestPacket mrRequestPacket, ExecutedCallback callback){
            this.mrRequestPacket = new MRRequestPacket(mrRequestPacket.getRequestID(), mrRequestPacket.getPacketType(), mrRequestPacket);
            this.callback = callback;
        }

        @Override
        public String toString(){
            return this.mrRequestPacket +" ["+ callback+"]";
        }
//        Once an ack is received the response writes are added to the priority queue. Return true if the requestSent set is empty.
        public boolean ackReceived(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm){
            removeReqFromSet(mrRequestPacket.getSource());
            if(!mrRequestPacket.getResponseWrites().isEmpty()) {
                this.addWrites(mrRequestPacket.getResponseWrites().get(mrRequestPacket.getSource()));
            }
            if (this.requestSent.isEmpty()){
                return true;
            }
            return false;
        }
        public boolean isWrite(){
            System.out.println("isWrite: "+this.mrRequestPacket.getPacketType()+(this.mrRequestPacket.getPacketType() == MRRequestPacket.MRPacketType.WRITE));
            return this.mrRequestPacket.getPacketType() == MRRequestPacket.MRPacketType.WRITE;
        }
        public void addCurrentIfNeeded(Integer nodeID, Timestamp ts){
            if(this.mrRequestPacket.getPacketType() == MRRequestPacket.MRPacketType.WRITE){
                this.mrRequestPacket.addResponseWrites(nodeID, ts, this.mrRequestPacket.getRequestValue());
            }
        }
        public void setResponse(HashMap<Integer, Timestamp> responseVectorClock, String responseValue){
            this.mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.RESPONSE);
            this.mrRequestPacket.setResponseVectorClock(responseVectorClock);
            this.mrRequestPacket.setResponseValue(responseValue);
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
        public MRRequestPacket getMrRequestPacket() {
            return this.mrRequestPacket;
        }

        public Integer getRequestSentLength() {
            System.out.println(this.requestSent);
            return requestSent.size();
        }
    }
    private void handlePacket(MRRequestPacket qp, MRReplicatedStateMachine mrsm, ExecutedCallback callback){

        MRRequestPacket.MRPacketType packetType = qp.getType();

        switch(packetType) {
            case READ:
                handleReadRequest(qp, mrsm, callback);
                break;
            case WRITE:
                handleWriteRequest(qp, mrsm, callback);
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
     * @param mrRequestPacket
     * @param mrsm
     * @param callback
     */
    private void handleReadRequest(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm, ExecutedCallback callback){
        System.out.println("Read received: ---"+mrRequestPacket);
        this.requestsReceived.putIfAbsent(mrRequestPacket.getRequestID(), new MRRequestAndCallback(mrRequestPacket, callback));
        if(!mrRequestPacket.getRequestVectorClock().isEmpty()){
            mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.FWD);
            for (int i = 0; i < mrsm.getMembers().size(); i++) {
                if (mrsm.getMembers().get(i) != this.myID & mrRequestPacket.getRequestVectorClock().get(mrsm.getMembers().get(i)).compareTo(this.wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i))) > 0) {
                    if (!isNodeUp(mrsm.getMembers().get(i))){
                        this.requestsReceived.get(mrRequestPacket.getRequestID()).addWrites(mrRequestPacket.getRequestWrites().get(mrsm.getMembers().get(i)));
                    }
                    else {
                        mrRequestPacket.setWritesFrom(this.wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i)));
                        mrRequestPacket.setWritesTo(mrRequestPacket.getRequestVectorClock().get(mrsm.getMembers().get(i)));
                        mrRequestPacket.setSource(this.myID);
                        mrRequestPacket.setDestination(mrsm.getMembers().get(i));
                        this.requestsReceived.get(mrRequestPacket.getRequestID()).addReqToSet(mrRequestPacket.getDestination());
                        this.sendRequest(mrRequestPacket, mrRequestPacket.getDestination());
                    }
                }
            }
        }
        if(mrRequestPacket.getRequestVectorClock().isEmpty() || this.requestsReceived.get(mrRequestPacket.getRequestID()).getRequestSentLength() == 0) {
            System.out.println("Request vector clock is empty");
            mrRequestPacket.setResponseVectorClock(this.wvc.get(mrsm.getServiceName()));
            mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.FWD_ACK);
            mrRequestPacket.setSource(this.myID);
            this.requestsReceived.get(mrRequestPacket.getRequestID()).addReqToSet(mrRequestPacket.getSource());
            handlePacket(mrRequestPacket, mrsm, callback);
        }
    }

    /**
     * If the vector clock of the request is greater gets all the writes after it, else handles the request as an ack
     * @param mrRequestPacket
     * @param mrsm
     * @param callback
     */
    private void handleWriteRequest(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm, ExecutedCallback callback){
        System.out.println("Write received:---"+mrRequestPacket);
        this.requestsReceived.putIfAbsent(mrRequestPacket.getRequestID(), new MRRequestAndCallback(mrRequestPacket, callback));
        if(!mrRequestPacket.getRequestVectorClock().isEmpty()) {
            mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.FWD);
            for (int i = 0; i < mrsm.getMembers().size(); i++) {
                if (mrRequestPacket.getRequestVectorClock().get(mrsm.getMembers().get(i)).compareTo(this.wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i))) > 0) {
                    if (!isNodeUp(mrsm.getMembers().get(i))){
                        this.requestsReceived.get(mrRequestPacket.getRequestID()).addWrites(mrRequestPacket.getRequestWrites().get(mrsm.getMembers().get(i)));
                    }
                    else {
                        mrRequestPacket.setWritesFrom(this.wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i)));
                        mrRequestPacket.setSource(this.myID);
                        mrRequestPacket.setDestination(mrsm.getMembers().get(i));
                        this.requestsReceived.get(mrRequestPacket.getRequestID()).addReqToSet(mrRequestPacket.getDestination());
                        this.sendRequest(new MRRequestPacket(mrRequestPacket.getRequestID(), mrRequestPacket.getPacketType(), mrRequestPacket), mrRequestPacket.getDestination());
                    }
                }
            }
        }
        if (mrRequestPacket.getRequestVectorClock().isEmpty() || this.requestsReceived.get(mrRequestPacket.getRequestID()).getRequestSentLength() == 0) {
                mrRequestPacket.setResponseVectorClock(this.wvc.get(mrsm.getServiceName()));
                mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.FWD_ACK);
                mrRequestPacket.setSource(this.myID);
                this.requestsReceived.get(mrRequestPacket.getRequestID()).addReqToSet(mrRequestPacket.getSource());
                this.handlePacket(mrRequestPacket, mrsm, callback);
        }

    }

    /**
     * Adds writes from own write set to the response
     * @param mrRequestPacket
     * @param mrsm
     */
    private void handleFwdRequest(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm){
        this.heardFrom(mrRequestPacket.getSource());
        mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.FWD_ACK);
        if (mrRequestPacket.getWritesTo().equals(new Timestamp(0))){
            mrRequestPacket.setWritesTo(new Timestamp(System.currentTimeMillis()));
        }
        System.out.println("handle forward received:---"+mrRequestPacket);
        for (Write write: this.writesByServer.get(mrsm.getServiceName())){
            System.out.println("------------------------------------"+write);
            if((write.getTs().compareTo(mrRequestPacket.getWritesFrom()) >= 0) & (write.getTs().compareTo(mrRequestPacket.getWritesTo()) <= 0)){
                System.out.println("Adding to response writes: "+write.getStatement());
                mrRequestPacket.addResponseWrites(this.myID, write.getTs(), write.getStatement());
            }
        }
        int dest = mrRequestPacket.getDestination();
        mrRequestPacket.setDestination(mrRequestPacket.getSource());
        mrRequestPacket.setSource(dest);
        this.sendRequest(mrRequestPacket, mrRequestPacket.getDestination());
    }

    /**
     * If writes from all nodes are received, executes the writes in the priority queue. Updates vector clock and write set if needed
     * @param mrRequestPacket
     * @param mrsm
     */
    private void handleFwdAck(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm){
        this.heardFrom(mrRequestPacket.getSource());
        System.out.println("handle forward ack received:--"+mrRequestPacket);
        if(this.requestsReceived.get(mrRequestPacket.getRequestID()).ackReceived(mrRequestPacket, mrsm)){
            for (Write write : this.requestsReceived.get(mrRequestPacket.getRequestID()).getPq()){
                mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.WRITE);
                mrRequestPacket.setRequestValue(write.getStatement());
                this.wvc.get(mrsm.getServiceName()).put(write.getNode(), write.getTs());
                Request request = getInterfaceRequest(this.myApp, mrRequestPacket.toString());
                this.myApp.execute(request, false);
            }
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            System.out.println("after execution: "+this.wvc.get(mrsm.getServiceName()));
            this.requestsReceived.get(mrRequestPacket.getRequestID()).addCurrentIfNeeded(this.myID, ts);
            Request request = getInterfaceRequest(this.myApp, this.requestsReceived.get(mrRequestPacket.getRequestID()).getMrRequestPacket().toString());
            this.myApp.execute(request, false);
            assert request != null;
            if (this.requestsReceived.get(mrRequestPacket.getRequestID()).isWrite()) {
                if(((MRRequestPacket)request).getResponseValue().equals("EXECUTED")) {
                    this.wvc.get(mrsm.getServiceName()).put(this.myID, ts);
                    this.writesByServer.get(mrsm.getServiceName()).add(new Write(ts, this.requestsReceived.get(mrRequestPacket.getRequestID()).getMrRequestPacket().getRequestValue(), this.myID));
                }
            }
            this.requestsReceived.get(mrRequestPacket.getRequestID()).setResponse(this.wvc.get(mrsm.getServiceName()), ((MRRequestPacket)request).getResponseValue());
            sendResponse(mrRequestPacket.getRequestID(), this.requestsReceived.get(mrRequestPacket.getRequestID()).getMrRequestPacket());
        }
    }
    private void processFailureDetection(MRRequestPacket request) {
        try {
            FailureDetectionPacket<NodeIDType> failureDetectionPacket = new FailureDetectionPacket<>(new JSONObject(request.getRequestValue()), this.unstringer);
            FD.receive(failureDetectionPacket);
        }
        catch (Exception e){
            System.out.println("Exception: "+e);
        }
    }
    public void sendResponse(Long requestID, MRRequestPacket mrResponsePacket){
        MRRequestAndCallback requestAndCallback = this.requestsReceived.get(requestID);
        if (requestAndCallback != null && requestAndCallback.callback != null) {
            mrResponsePacket.setPacketType(MRRequestPacket.MRPacketType.RESPONSE);
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
    private void sendRequest(MRRequestPacket mrRequestPacket,
                             int nodeID){
        GenericMessagingTask<NodeIDType,?> gTask = null;
        System.out.println("sending req:--"+mrRequestPacket+" to node "+nodeID);
        try {
            // forward to nodeID
            gTask = new GenericMessagingTask(this.integerMap.get(nodeID),
                    mrRequestPacket);
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
    private synchronized MRReplicatedStateMachine createReplicatedQuorumFinal(
            String serviceName, int version, Set<NodeIDType> nodes,
            Replicable app, String initialState){
        MRReplicatedStateMachine mrrsm = this.getInstance(serviceName);
        if (mrrsm != null)
            return mrrsm;
        try {
            mrrsm = new MRReplicatedStateMachine(serviceName, version, myID,
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
        MRReplicatedStateMachine mrsm = this.getInstance(serviceName);
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
        if(request.getRequestType() == MRRequestPacket.MRPacketType.FAILURE_DETECT){
            this.handlePacket((MRRequestPacket)request, null, callback);
            return null;
        }
        MRRequestPacket mrRequestPacket = this.getMRRequestPacket(request);

        boolean matched = false;

        MRReplicatedStateMachine mrsm = this.getInstance(serviceName);

        if (mrsm != null) {
            matched = true;
            assert mrRequestPacket != null;
            mrRequestPacket.setServiceName(serviceName);
            this.handlePacket(mrRequestPacket, mrsm, callback);
        } else {
            System.out.println("The given quorumID "+serviceName+" has no state machine associated");
        }


        return matched ? mrsm.getServiceName() : null;
    }
    private MRRequestPacket getMRRequestPacket(Request request){
        try {
            return (MRRequestPacket) request;
        }
        catch (Exception e){
            System.out.println(e.toString());
        }
        return null;
    }
    private MRReplicatedStateMachine getInstance(String quorumID){
        return this.replicatedSM.get(quorumID);
    }
    private void putInstance(String quorumID, MRReplicatedStateMachine mrrsm){
        this.replicatedSM.put(quorumID, mrrsm);
    }
    private void putVectorClock(String serviceName, MRReplicatedStateMachine mrrsm){
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
        MRReplicatedStateMachine mrsm = this.getInstance(quorumID);
        if ( mrsm != null)
            return (int) mrsm.getVersion();
        return -1;
    }
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        MRReplicatedStateMachine mrsm = this.getInstance(serviceName);
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
