package edu.umass.cs.consistency.MonotonicReads;

import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.consistency.EventualConsistency.DynamoApp;
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

import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MRManager<NodeIDType> {
    private final PaxosMessenger<NodeIDType> messenger; // messaging
    private final int myID;
    private final Replicable myApp;
    private final HashMap<String, MRReplicatedStateMachine> replicatedSM = new HashMap<>();
    private final HashMap<String, ArrayList<Write>> writesByServer = new HashMap<>();
    private final HashMap<String, HashMap<Integer, Timestamp>> wvc = new HashMap<>();
    private HashMap<Long, MRRequestAndCallback> requestsReceived = new HashMap<Long, MRRequestAndCallback>();
    private ArrayList<String> serviceNames = new ArrayList<String>();
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    private final Stringifiable<NodeIDType> unstringer;
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
    }
    public static class Write{
        private String statement;
        private Timestamp ts;
        Write(Timestamp ts, String statement){
            this.statement = statement;
            this.ts = ts;
        }
//        write a toString for this class
        public String getStatement(){
            return this.statement;
        }

        public Timestamp getTs() {
            return ts;
        }
    }
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
    public class MRRequestAndCallback {
        protected MRRequestPacket mrRequestPacket;
        final ExecutedCallback callback;
        protected Set<Integer> requestSent = new HashSet<>();
        PriorityQueue<Write> pq = new PriorityQueue<Write>(new WriteComparator());

        MRRequestAndCallback(MRRequestPacket mrRequestPacket, ExecutedCallback callback){
            this.mrRequestPacket = mrRequestPacket;
            this.callback = callback;
        }

        @Override
        public String toString(){
            return this.mrRequestPacket +" ["+ callback+"]";
        }
        public boolean ackReceived(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm){
            removeReqFromSet(mrRequestPacket.getSource());
            for (Timestamp key: mrRequestPacket.getResponseWrites().keySet()){
                pq.add(new Write(key, mrRequestPacket.getResponseWrites().get(key)));
                mrRequestPacket.addResponseWrites(key, mrRequestPacket.getResponseWrites().get(key));
            }
            if (this.requestSent.isEmpty()){
                return true;
            }
            return false;
        }
        public void addCurrentIfNeeded(Timestamp ts){
            if(this.mrRequestPacket.getPacketType() == MRRequestPacket.MRPacketType.WRITE){
                this.mrRequestPacket.addResponseWrites(ts, this.mrRequestPacket.getRequestValue());
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
        public MRRequestPacket getMrRequestPacket() {
            return this.mrRequestPacket;
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
            default:
                break;
        }

    }
    private void handleReadRequest(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm, ExecutedCallback callback){
        this.requestsReceived.putIfAbsent(mrRequestPacket.getRequestID(), new MRRequestAndCallback(mrRequestPacket, callback));
        mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.FWD);
        for (int i = 0; i < mrsm.getMembers().size(); i++){
            if ( mrRequestPacket.getRequestVectorClock().get(i).compareTo(this.wvc.get(mrsm.getServiceName()).get(i)) > 0){
                mrRequestPacket.setWritesFrom(this.wvc.get(mrsm.getServiceName()).get(i));
                mrRequestPacket.setWritesTo(mrRequestPacket.getRequestVectorClock().get(i));
                mrRequestPacket.setSource(this.myID);
                mrRequestPacket.setDestination(mrsm.getMembers().get(i));
                this.sendRequest(mrRequestPacket, mrRequestPacket.getDestination());
            }
        }
    }
    private void handleWriteRequest(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm, ExecutedCallback callback){
        this.requestsReceived.putIfAbsent(mrRequestPacket.getRequestID(), new MRRequestAndCallback(mrRequestPacket, callback));
        mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.FWD_ACK);
        for (int i = 0; i < mrsm.getMembers().size(); i++){
            if (mrRequestPacket.getRequestVectorClock().get(i).compareTo(this.wvc.get(mrsm.getServiceName()).get(i)) > 0){
                mrRequestPacket.setWritesFrom(this.wvc.get(mrsm.getServiceName()).get(i));
                mrRequestPacket.setSource(this.myID);
                mrRequestPacket.setDestination(mrsm.getMembers().get(i));
                this.sendRequest(mrRequestPacket, mrRequestPacket.getDestination());
            }
        }
    }
    private void handleFwdRequest(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm){
        mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.FWD_ACK);
        if (mrRequestPacket.getWritesTo() == null){
            mrRequestPacket.setWritesTo(new Timestamp(System.currentTimeMillis()));
        }
        for (Write write: this.writesByServer.get(mrsm.getServiceName())){
            if((write.getTs().compareTo(mrRequestPacket.getWritesFrom()) > 0) & (write.getTs().compareTo(mrRequestPacket.getWritesTo()) < 0)){
                mrRequestPacket.addResponseWrites(write.getTs(), write.getStatement());
            }
        }
        int dest = mrRequestPacket.getDestination();
        mrRequestPacket.setDestination(mrRequestPacket.getSource());
        mrRequestPacket.setSource(dest);
        this.sendRequest(mrRequestPacket, mrRequestPacket.getDestination());
    }
    private void handleFwdAck(MRRequestPacket mrRequestPacket, MRReplicatedStateMachine mrsm){
        if(this.requestsReceived.get(mrRequestPacket.getRequestID()).ackReceived(mrRequestPacket, mrsm)){
            for (Write write : this.requestsReceived.get(mrRequestPacket.getRequestID()).getPq()){
                mrRequestPacket.setPacketType(MRRequestPacket.MRPacketType.WRITE);
                mrRequestPacket.setRequestValue(write.getStatement());
                Request request = getInterfaceRequest(this.myApp, mrRequestPacket.toString());
                this.myApp.execute(request, false);
            }
        }
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        this.wvc.get(mrsm.getServiceName()).put(this.myID, ts);
        this.requestsReceived.get(mrRequestPacket.getRequestID()).addCurrentIfNeeded(ts);
        Request request = getInterfaceRequest(this.myApp, this.requestsReceived.get(mrRequestPacket.getRequestID()).getMrRequestPacket().getRequestValue());
        this.myApp.execute(request, false);
        assert request != null;
        this.requestsReceived.get(mrRequestPacket.getRequestID()).setResponse(this.wvc.get(mrsm.getServiceName()), ((MRRequestPacket)request).getResponseValue());
        sendResponse(mrRequestPacket.getRequestID(), this.requestsReceived.get(mrRequestPacket.getRequestID()).getMrRequestPacket());
    }
    public void sendResponse(Long requestID, MRRequestPacket mrResponsePacket){
        MRRequestAndCallback requestAndCallback = this.requestsReceived.get(requestID);
        if (requestAndCallback != null && requestAndCallback.callback != null) {

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
            System.out.println("Creating new Replicated Quorum State Machine: "+ mrrsm);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        this.putInstance(serviceName, mrrsm);
        this.integerMap.put(nodes);
        this.putVectorClock(serviceName, mrrsm);
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

        MRRequestPacket mrRequestPacket = this.getMRRequestPacket(request);
        boolean matched = false;

        MRReplicatedStateMachine mrsm = this.getInstance(serviceName);

        if (mrsm != null) {
            matched = true;
            assert mrRequestPacket != null;
            mrRequestPacket.setServiceName(serviceName);
            this.handlePacket(mrRequestPacket, mrsm, callback);
        } else {
            System.out.println("The given quorumID has no state machine associated");
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
}
