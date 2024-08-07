package edu.umass.cs.consistency.EventualConsistency.Domain;

import edu.umass.cs.consistency.EventualConsistency.DynamoRequestPacket;

import java.util.ArrayList;
import java.util.HashMap;

public class GraphNode {
    private ArrayList<RequestInformation> requests;
    private HashMap<Integer, Integer> vectorClock;
    private ArrayList<GraphNode> children;
    public GraphNode() {
        vectorClock = new HashMap<>();
        children =  new ArrayList<GraphNode>();
        requests = new ArrayList<>();
    }
    public GraphNode(HashMap<Integer, Integer> vectorClock) {
        this.vectorClock = vectorClock;
        children =  new ArrayList<GraphNode>();
        requests = new ArrayList<>();
    }

    public void addChildNode(GraphNode graphNode){
        this.children.add(graphNode);
    }

    public void setVectorClock(HashMap<Integer, Integer> vectorClock) {
        this.vectorClock = vectorClock;
    }

    public HashMap<Integer, Integer> getVectorClock() {
        return vectorClock;
    }

    public ArrayList<RequestInformation> getRequests() {
        return requests;
    }

    public void setRequests(ArrayList<RequestInformation> requests) {
        this.requests = requests;
    }
    public void addRequests(DynamoRequestPacket dynamoRequestPacket){
        requests.add(new RequestInformation(dynamoRequestPacket.getRequestID(), dynamoRequestPacket.getType().getLabel() + " " + dynamoRequestPacket.getRequestValue()));
        if (!dynamoRequestPacket.getAllRequests().isEmpty()) {
            addAllRequests(dynamoRequestPacket.getAllRequests());
        }
    }
    public void addRequest(DynamoRequestPacket dynamoRequestPacket){
        requests.add(new RequestInformation(dynamoRequestPacket.getRequestID(), dynamoRequestPacket.getType().getLabel() + " " + dynamoRequestPacket.getRequestValue()));
    }
    private void addAllRequests(HashMap<Long, String> allRequests){
        for(Long reqId: allRequests.keySet()){
            requests.add(new RequestInformation(reqId, allRequests.get(reqId)));
        }
    }

    public ArrayList<GraphNode> getChildren() {
        return children;
    }
    public boolean isDominant(GraphNode graphNode){
        for (int key : this.getVectorClock().keySet()) {
            if(this.getVectorClock().get(key) < graphNode.getVectorClock().get(key)){
                return false;
            }
        }
        return true;
    }

    public boolean isDominant(HashMap <Integer, Integer> vectorClock){
        for (int key : this.getVectorClock().keySet()) {
            if(this.getVectorClock().get(key) < vectorClock.get(key)){
                return false;
            }
        }
        return true;
    }

    public boolean isMinor(HashMap <Integer, Integer> vectorClock){
        System.out.println(this.getVectorClock()+" "+vectorClock);
        for (int key : this.getVectorClock().keySet()) {
            if(this.getVectorClock().get(key) >= vectorClock.get(key)){
                return false;
            }
        }
        return true;
    }

}
