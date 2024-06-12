package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.MonotonicReads.FailureDetection;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatusReport<NodeIDType> {
    private final ScheduledExecutorService execpool;
    private final NodeIDType myID;
    private final InterfaceNIOTransport<NodeIDType, JSONObject> nioTransport;
    private Logger log = Logger.getLogger(StatusReport.class.getName());
    private HashMap<NodeIDType, ScheduledFuture<PingTask>> futures;
    StatusReport(NodeIDType id,
                 InterfaceNIOTransport<NodeIDType, JSONObject> niot){
        this.nioTransport = niot;
        this.myID = id;
//        this.serviceName = serviceName;
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
    public synchronized void sendKeepAlive(NodeIDType id, StatusReportPacket statusReportPacket) {
        try {
            PingTask pingTask = new PingTask(id, statusReportPacket.toJSONObject(),
                    this.nioTransport);

            pingTask.run(); // run once immediately
            ScheduledFuture<?> future = execpool
                    .scheduleAtFixedRate(pingTask,
                            10,
                            100,
                            TimeUnit.MILLISECONDS);
            futures.put(
                    id,
                    (ScheduledFuture<PingTask>) future);

        } catch (JSONException e) {
            log.severe("Can not create ping packet at node " + this.myID
                    + " for node " + id);
            e.printStackTrace();
        }
    }
    class PingTask implements Runnable {
        private final NodeIDType destID;
        private final JSONObject pingJson;
        private final InterfaceNIOTransport<NodeIDType, JSONObject> nioTransport;

        PingTask(NodeIDType id, JSONObject fdpJson,
                 InterfaceNIOTransport<NodeIDType, JSONObject> niot) {
            destID = id;
            pingJson = fdpJson;
            nioTransport = niot;
        }

        public void run() {
            try {
                if(!StatusReport.this.execpool.isShutdown())
                    nioTransport.sendToID(destID, pingJson);
            } catch (IOException e) {
                try {
                    log.log(Level.INFO,
                            "{0} encountered IOException while sending keepalive to {2}",
                            new Object[] { pingJson.getInt("sender"), destID });
                } catch (JSONException je) {
                    e.printStackTrace();
                }
            }
        }
    }

}
