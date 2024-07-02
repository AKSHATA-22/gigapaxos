package edu.umass.cs.consistency.EventualConsistency.Domain;

import edu.umass.cs.consistency.EventualConsistency.DynamoRequestPacket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

public class DAG {
    private GraphNode rootNode;

    public DAG(HashMap<Integer, Integer> vectorClock) {
        this.rootNode = new GraphNode(null, vectorClock );
    }

    public ArrayList<GraphNode> latestNodesForGivenObject(String objectKey){
        ArrayList<GraphNode> latestNodes = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        latestNodes.add(rootNode);
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            if(!current.getRequests().isEmpty() && current.getObjectKey().equals(objectKey)){
                addNode(latestNodes, current);
            }
            for(GraphNode childNode: current.getChildren()){
                if(childNode != null)
                    graphNodeStack.push(childNode);
            }
        }
        return latestNodes;
    }
    public static GraphNode createDominantChildGraphNode(ArrayList<GraphNode> graphNodesList, DynamoRequestPacket dynamoRequestPacket){
        GraphNode dominantChildNode = new GraphNode();
        if(dynamoRequestPacket != null){
            dominantChildNode.setObjectKey(dynamoRequestPacket.getRequestValue());
        }
        HashMap<Integer, Integer> vectorClock = new HashMap<>();
        for(GraphNode graphNode: graphNodesList){
            if(vectorClock.isEmpty()){
                for(Integer key: graphNode.getVectorClock().keySet()){
                    vectorClock.put(key, graphNode.getVectorClock().get(key));
                }
            }
            else{
                for(Integer key: vectorClock.keySet()){
                    if(vectorClock.get(key) < graphNode.getVectorClock().get(key)){
                        vectorClock.put(key, graphNode.getVectorClock().get(key));
                    }
                }
            }
            graphNode.addChildNode(dominantChildNode);
        }
        dominantChildNode.setVectorClock(vectorClock);
        return dominantChildNode;
    }
    public static void addChildNode(ArrayList<GraphNode> graphNodesList, GraphNode childNode){
        for(GraphNode graphNode: graphNodesList){
            graphNode.addChildNode(childNode);
        }
    }

    public ArrayList<GraphNode> latestNodesWithVectorClockAsDominant(GraphNode receivedGraphNode, boolean isPut){
        ArrayList<GraphNode> latestNodes = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        latestNodes.add(rootNode);
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            if(current.isDominant(receivedGraphNode) && isPut){
                return null;
            }
            if((current.getObjectKey() == null) || (current.getObjectKey().equals(receivedGraphNode.getObjectKey()) && receivedGraphNode.isDominant(current))){
                addNode(latestNodes, current);
            }
            for(GraphNode childNode: current.getChildren()){
                if(childNode != null)
                    graphNodeStack.push(childNode);
            }
        }
        return latestNodes;
    }

    private void addNode(ArrayList<GraphNode> latestNodes, GraphNode toBeCompared){
        for (int i = 0; i < latestNodes.size(); i++){
            if(toBeCompared.isDominant(latestNodes.get(i))){
                latestNodes.remove(latestNodes.get(i));
            }
        }
        latestNodes.add(toBeCompared);
    }
    public HashMap<Long, String> getAllRequests(String objectKey){
        HashMap<Long, String> mapOfRequests = new HashMap<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            if(!current.getRequests().isEmpty() && current.getObjectKey().equals(objectKey)){
                for(RequestInformation requestInformation: current.getRequests()){
                    mapOfRequests.put(requestInformation.getRequestID(), requestInformation.getRequestQuery());
                }
            }
            for(GraphNode childNode: current.getChildren()){
                if(childNode != null)
                    graphNodeStack.push(childNode);
            }
        }
        return mapOfRequests;
    }
    public HashMap<Long, String> getAllRequestsWithVectorClockAsDominant(GraphNode graphNode){
        HashMap<Long, String> mapOfRequests = new HashMap<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            if(!current.getRequests().isEmpty() && current.getObjectKey().equals(graphNode.getObjectKey()) && graphNode.isDominant(current)){
                for(RequestInformation requestInformation: current.getRequests()){
                    mapOfRequests.put(requestInformation.getRequestID(), requestInformation.getRequestQuery());
                }
            }
            for(GraphNode childNode: current.getChildren()){
                if(childNode != null)
                    graphNodeStack.push(childNode);
            }
        }
        return mapOfRequests;
    }
    public ArrayList<HashMap<Integer, Integer>> getAllVC(String objectKey){
        ArrayList<HashMap<Integer, Integer>> allVC = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            if(!current.getRequests().isEmpty() && current.getObjectKey().equals(objectKey)){
                allVC.add(current.getVectorClock());
            }
            for(GraphNode childNode: current.getChildren()){
                if(childNode != null)
                    graphNodeStack.push(childNode);
            }
        }
        return allVC;
    }

    public GraphNode getRootNode() {
        return rootNode;
    }

    public void setRootNode(GraphNode rootNode) {
        this.rootNode = rootNode;
    }
}
