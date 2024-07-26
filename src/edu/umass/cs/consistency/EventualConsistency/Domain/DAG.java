package edu.umass.cs.consistency.EventualConsistency.Domain;

import edu.umass.cs.consistency.EventualConsistency.DynamoManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import java.util.logging.Level;

public class DAG {
    private GraphNode rootNode;

    public DAG(HashMap<Integer, Integer> vectorClock) {
        this.rootNode = new GraphNode(vectorClock);
    }

    public ArrayList<GraphNode> getLatestNodes(){
        ArrayList<GraphNode> latestNodes = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            visited.add(current.getVectorClock());
            if(current.getChildren().isEmpty()){
                latestNodes.add(current);
                continue;
            }
            for(GraphNode childNode: current.getChildren()){
                if(childNode != null && !visited.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return latestNodes;
    }

    public ArrayList<GraphNode> latestNodesWithVectorClockAsDominant(GraphNode receivedGraphNode, boolean isPut){
        ArrayList<GraphNode> latestNodes = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        graphNodeStack.push(rootNode);
        DynamoManager.log.log(Level.INFO, "Received node: {0}", new Object[]{receivedGraphNode.getVectorClock()});
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            if(current.isDominant(receivedGraphNode) && isPut){
                return null;
            }
            visited.add(current.getVectorClock());
            if(receivedGraphNode.isDominant(current)){
                addNode(latestNodes, current);
                DynamoManager.log.log(Level.INFO, "Adding Current node: {0}, children: {1}, isEmpty: {2}", new Object[]{current.getVectorClock(), current.getChildren(), current.getChildren().isEmpty()});
                continue;
            }
            for(GraphNode childNode: current.getChildren()){
                if (!visited.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return latestNodes;
    }
    public static GraphNode createDominantChildGraphNode(ArrayList<GraphNode> graphNodesList, HashMap<Integer, Integer> vectorClock){
        GraphNode dominantChildNode = new GraphNode();
        for(GraphNode graphNode: graphNodesList){
            for(Integer key: vectorClock.keySet()){
                if(vectorClock.get(key) < graphNode.getVectorClock().get(key)){
                    vectorClock.put(key, graphNode.getVectorClock().get(key));
                }
            }
            graphNode.addChildNode(dominantChildNode);
        }
        dominantChildNode.setVectorClock(vectorClock);
        return dominantChildNode;
    }

    public static GraphNode getDominantVC(ArrayList<GraphNode> graphNodesList){
        HashMap<Integer, Integer> dominantVC = graphNodesList.get(0).getVectorClock();
        for (int i = 1; i < graphNodesList.size(); i++) {
            for(Integer key: dominantVC.keySet()){
                if(dominantVC.get(key) < graphNodesList.get(i).getVectorClock().get(key)){
                    dominantVC.put(key, graphNodesList.get(i).getVectorClock().get(key));
                }
            }
        }
        return new GraphNode(dominantVC);
    }

    public static void addChildNode(ArrayList<GraphNode> graphNodesList, GraphNode childNode){
        for(GraphNode graphNode: graphNodesList){
            graphNode.addChildNode(childNode);
        }
    }

    private void addNode(ArrayList<GraphNode> latestNodes, GraphNode toBeCompared){
        for (int i = 0; i < latestNodes.size(); i++){
            if(toBeCompared.isDominant(latestNodes.get(i))){
                latestNodes.remove(latestNodes.get(i));
            }
        }
        latestNodes.add(toBeCompared);
    }
    public HashMap<Long, String> getAllRequests(HashMap <Integer, Integer> vectorClock){
        HashMap<Long, String> mapOfRequests = new HashMap<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            visited.add(current.getVectorClock());
            if(!current.getRequests().isEmpty() && current.isMinor(vectorClock)){
                for(RequestInformation requestInformation: current.getRequests()){
                    mapOfRequests.put(requestInformation.getRequestID(), requestInformation.getRequestQuery());
                }
            }
            for(GraphNode childNode: current.getChildren()){
                if(childNode != null && !visited.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return mapOfRequests;
    }

    public HashMap<Integer, Integer> getMinimumLatestNode(HashMap<Integer, Integer> maxVectorClock){
        ArrayList<GraphNode> latestNodes = getLatestNodes();
        for (GraphNode graphNode: latestNodes){
            maxVectorClock.replaceAll((k, v) -> Math.max(maxVectorClock.get(k), graphNode.getVectorClock().get(k)));
        }
        return maxVectorClock;
    }

    public void pruneRequests(HashMap<Integer, Integer> vectorClock){
        ArrayList<GraphNode> nodes = new ArrayList<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            visited.add(current.getVectorClock());
            if(current.isDominant(vectorClock)){
                nodes.add(current);
            } else if (current.isMinor(vectorClock)) {
                for(GraphNode childNode: current.getChildren()){
                    if(childNode != null && !visited.contains(childNode.getVectorClock()))
                        graphNodeStack.push(childNode);
                }
                current.setRequests(null);
            }
        }
        for(GraphNode node: nodes){
            rootNode.addChildNode(node);
        }
    }
    public HashMap<Long, String> getAllRequestsWithVectorClockAsDominant(GraphNode graphNode){
        HashMap<Long, String> mapOfRequests = new HashMap<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            visited.add(current.getVectorClock());
            if(!current.getRequests().isEmpty() && graphNode.isDominant(current)){
                for(RequestInformation requestInformation: current.getRequests()){
                    mapOfRequests.put(requestInformation.getRequestID(), requestInformation.getRequestQuery());
                }
            }
            for(GraphNode childNode: current.getChildren()){
                if(childNode != null && !visited.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return mapOfRequests;
    }
    public ArrayList<HashMap<Integer, Integer>> getAllVC(){
        ArrayList<HashMap<Integer, Integer>> allVC = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()){
            GraphNode current = graphNodeStack.pop();
            allVC.add(current.getVectorClock());
            for(GraphNode childNode: current.getChildren()){
                if(childNode != null && !allVC.contains(childNode.getVectorClock()))
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
