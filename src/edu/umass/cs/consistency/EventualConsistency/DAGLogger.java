package edu.umass.cs.consistency.EventualConsistency;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;

public class DAGLogger {
    private String logFile;
    private String state;
    private String quorumId;
    private HashMap<Integer, Integer> vectorClock;
    private final String directoryPath = "logs/DAGLogs";

    public DAGLogger(String logFile) throws IOException {
        this.logFile = logFile;
        File file = new File(directoryPath+"/"+this.logFile);
        if (!file.exists()) {
            if (file.createNewFile()) {
                DynamoManager.log.log(Level.INFO, "Log File created: {0}", new Object[]{file.getAbsolutePath()});
            } else {
                DynamoManager.log.log(Level.WARNING, "Failed to create log file: {0}", new Object[]{file.getAbsolutePath()});
            }
        }
    }

    public void checkpoint(String state, HashMap<Integer, Integer> vectorClock, String quorumId) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        this.state = state;
        this.vectorClock = vectorClock;
        jsonObject.put("state", state);
        jsonObject.put("vectorClock", vectorClock);
        jsonObject.put("quorumId", quorumId);

        try (FileWriter fileWriter = new FileWriter(directoryPath+"/"+this.logFile)) {
            fileWriter.write(jsonObject.toString());
            DynamoManager.log.log(Level.INFO, "Checkpointing completed successfully");
        } catch (IOException e) {
            DynamoManager.log.log(Level.SEVERE, "Could not checkpoint the state");
            throw new RuntimeException(e);
        }
    }

    public void restore() throws JSONException {
        File file = new File(directoryPath+"/"+this.logFile);
        StringBuilder content = new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                content.append(line);
            }
        } catch (IOException e) {
            DynamoManager.log.log(Level.SEVERE, "Could not read the log file {0}", new Object[]{this.logFile});
            throw new RuntimeException(e);
        }
        if(content.isEmpty()){
            DynamoManager.log.log(Level.WARNING, "The log file {0} is empty", new Object[]{this.logFile});
            throw new RuntimeException("The file is empty");
        }

        JSONObject jsonObject = new JSONObject(content.toString());
        this.state = jsonObject.getString("state");
        this.quorumId = jsonObject.getString("quorumId");
        JSONObject vectorClockJson = jsonObject.getJSONObject("vectorClock");
        this.vectorClock = new HashMap<>();

        if (vectorClockJson.length() != 0) {
            for (Iterator it = vectorClockJson.keys(); it.hasNext(); ) {
                int i = Integer.parseInt(it.next().toString());
                this.vectorClock.put(i, Integer.parseInt(vectorClockJson.get(String.valueOf(i)).toString()));
            }
        }
        DynamoManager.log.log(Level.INFO, "Restoration completed successfully");
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getQuorumId() {
        return quorumId;
    }

    public void setQuorumId(String quorumId) {
        this.quorumId = quorumId;
    }

    public HashMap<Integer, Integer> getVectorClock() {
        return vectorClock;
    }

    public void setVectorClock(HashMap<Integer, Integer> vectorClock) {
        this.vectorClock = vectorClock;
    }
}
