package edu.umass.cs.consistency.EventualConsistency;

import com.fasterxml.jackson.core.type.TypeReference;
import edu.umass.cs.consistency.EventualConsistency.Domain.CheckpointLog;
import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;
import edu.umass.cs.consistency.EventualConsistency.Domain.RequestInformation;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import com.fasterxml.jackson.databind.*;

public class DAGLogger {
    private String logFile;
    private String rollForwardLogFile;
    private CheckpointLog checkpointLog;
    private final String directoryPath = "logs/DAGLogs";

    public DAGLogger(String logFile) throws IOException {
        this.logFile = "/checkpoint."+logFile+".json";
        this.rollForwardLogFile = "/rollForward."+logFile+".txt";
        createNewFileIfNotExists(new File(directoryPath+this.logFile));
        createNewFileIfNotExists(new File(directoryPath+this.rollForwardLogFile));
    }

    private void reinitializeFile(String filePath) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, false))) {
            writer.write("");
        }
    }

    private void createNewFileIfNotExists(File file) throws IOException {
        if (!file.exists()) {
            if (file.createNewFile()) {
                DynamoManager.log.log(Level.INFO, "Log File created: {0}", new Object[]{file.getAbsolutePath()});
            } else {
                DynamoManager.log.log(Level.WARNING, "Failed to create log file: {0}", new Object[]{file.getAbsolutePath()});
            }
        }
        else {
            DynamoManager.log.log(Level.INFO, "Log file already exists: {0}", new Object[]{file.getAbsolutePath()});
//            reinitializeFile(file.getPath());
        }
    }

    public void rollForward(GraphNode graphNode){
        ObjectMapper objectMapper = new ObjectMapper();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(directoryPath+this.rollForwardLogFile, true))) {
            String jsonMap = objectMapper.writeValueAsString(graphNode.getVectorClock());
            String requests = objectMapper.writeValueAsString(graphNode.getRequests());
            writer.write(jsonMap + " " + requests);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ArrayList<GraphNode> readFromRollForwardFile() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> lines = Files.readAllLines(Path.of(directoryPath + this.rollForwardLogFile));
        ArrayList<GraphNode> graphNodes = new ArrayList<>();
        for (String line : lines) {
            String[] graphNodeAsString = line.split(" ");
            HashMap<Integer, Integer> vectorClock = objectMapper.readValue(graphNodeAsString[0], new TypeReference<HashMap<Integer, Integer>>() {});
            ArrayList<RequestInformation> requests = objectMapper.readValue(graphNodeAsString[1], new TypeReference<ArrayList<RequestInformation>>() {});
            graphNodes.add(new GraphNode(vectorClock, requests));
        }
        return graphNodes;
    }

    public void checkpoint(String state, int noOOfCheckpoints,  HashMap<Integer, Integer> vectorClock, String quorumId, ArrayList<GraphNode> leafNodes) throws JSONException {
        ArrayList<HashMap<Integer, Integer>> latestVectorClocks = new ArrayList<>();
        for (GraphNode node : leafNodes) {
            latestVectorClocks.add(node.getVectorClock());
        }
        this.checkpointLog = new CheckpointLog(state, noOOfCheckpoints, quorumId, vectorClock, latestVectorClocks);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.writeValue(new File(directoryPath+this.logFile), this.checkpointLog);
            reinitializeFile(directoryPath + this.rollForwardLogFile);
        } catch (Exception e) {
            DynamoManager.log.log(Level.SEVERE, "Could not checkpoint the state");
            throw new RuntimeException(e);
        }
        DynamoManager.log.log(Level.INFO, "Checkpointing completed successfully");
    }

    public synchronized CheckpointLog restore() throws JSONException, RuntimeException {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            File file = new File(directoryPath+this.logFile);
            if (file.exists()) {
                System.out.println("Trying to restore File :"+file.getAbsolutePath());
                this.checkpointLog = objectMapper.readValue(file, CheckpointLog.class);
                return this.checkpointLog;
            }
        } catch (IOException e) {
            DynamoManager.log.log(Level.SEVERE, "Error encountered: "+e);
            throw new RuntimeException("Restoration not completed");
        }
        return null;
    }

    public void setCheckpointLog(CheckpointLog checkpointLog) throws IOException {
        this.checkpointLog = checkpointLog;
        reinitializeFile(directoryPath + this.rollForwardLogFile);
    }

    public CheckpointLog getCheckpointLog() {
        return checkpointLog;
    }
}
