package edu.umass.cs.consistency.ClientCentric;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import edu.umass.cs.utils.Config;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class FailureDetection<NodeIDType> {
    // final static
    // 1 ping per 100ms total at each node
    private static final double MAX_FAILURE_DETECTION_TRAFFIC = 1 / 100.0;
    // pings randomly spaced within inter_ping_period_millis times this factor
    private static final double PING_PERTURBATION_FACTOR = 0.25;

    // static
    private static long node_detection_timeout_millis = Config
            .getGlobalLong(PaxosConfig.PC.FAILURE_DETECTION_TIMEOUT) * 1000;
    private static long inter_ping_period_millis = node_detection_timeout_millis / 2;

    private static long pessimism_offset = 0;

    private final long initTime = System.currentTimeMillis()
            - getPessimismOffset();

    // final
    private final ScheduledExecutorService execpool;
    private final NodeIDType myID;
    private final InterfaceNIOTransport<NodeIDType, JSONObject> nioTransport;
//    private final String serviceName;

    // non-final
    private Set<NodeIDType> keepAliveTargets;
    private ConcurrentHashMap<NodeIDType, Long> lastHeardFrom;
    private HashMap<NodeIDType, ScheduledFuture<PingTask>> futures;

    private static Logger log = Logger.getLogger(PaxosManager.class
            .getName());

    FailureDetection(NodeIDType id,
                     InterfaceNIOTransport<NodeIDType, JSONObject> niot,
                     String paxosLogFolder) {
        this.nioTransport = niot;
        this.myID = id;
//        this.serviceName = serviceName;
        this.execpool = Executors.newScheduledThreadPool(1,
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = Executors.defaultThreadFactory()
                                .newThread(r);
                        thread.setName(edu.umass.cs.gigapaxos.FailureDetection.class.getSimpleName()
                                + myID);
                        return thread;
                    }
                });
        lastHeardFrom = new ConcurrentHashMap<NodeIDType, Long>();
        keepAliveTargets = new TreeSet<NodeIDType>();
        futures = new HashMap<NodeIDType, ScheduledFuture<PingTask>>();
        initialize(paxosLogFolder);
    }


    protected synchronized static long getPessimismOffset() {
        return pessimism_offset;
    }

    public void close() {
        this.execpool.shutdownNow();
    }

    // should really not be taking this from outside
    private void initialize(String paxosLogFolder) {
        if (paxosLogFolder == null)
            return;
    }

    // makes sure that FD params are reasonable
    private synchronized boolean adjustFDParams() {
        boolean adjusted = false;
        int numMonitored = this.keepAliveTargets.size();
        double load = ((double) numMonitored) / inter_ping_period_millis;
        if (load > MAX_FAILURE_DETECTION_TRAFFIC) {
            inter_ping_period_millis = (long) (numMonitored
                    / MAX_FAILURE_DETECTION_TRAFFIC + 1); // +1 for strictly <
            node_detection_timeout_millis = inter_ping_period_millis * 2;
            assert (inter_ping_period_millis > 0);
            adjusted = true;
        }
        /*
         * If there was any adjustment above, we need to kill and restart
         * periodic ping tasks because there is no way to just change their
         * period midway.
         */

        // copy easiest way to avoid concurrent modification exceptions
        Set<NodeIDType> copy = new HashSet<NodeIDType>(this.keepAliveTargets);
        if (adjusted) {
            for (NodeIDType id : copy) {
                dontSendKeepAlive(id);
                // single-depth recursive call to adjustFDParams
                sendKeepAlive(id);
            }
        }
        return adjusted;
    }

    public void sendKeepAlive(NodeIDType[] nodes) {
        for (int i = 0; i < nodes.length; i++) {
            sendKeepAlive(nodes[i]);
        }
    }

    public void sendKeepAlive(Set<NodeIDType> nodes) {
        for (NodeIDType id : nodes) {
            sendKeepAlive(id);
        }
    }

    protected synchronized boolean dontSendKeepAlive(NodeIDType id) {
        if (this.futures.containsKey(id)) {
            ScheduledFuture<PingTask> pingTask = futures.get(id);
            pingTask.cancel(true);
            futures.remove(id);
            return true;
        }
        return false;
    }

    /*
     * Synchronized as it touches keepAliveTargets.
     */
    @SuppressWarnings("unchecked")
    public synchronized void sendKeepAlive(NodeIDType id) {
        if (!this.keepAliveTargets.contains(id))
            this.keepAliveTargets.add(id);
        try {
            if (!this.futures.containsKey(id)) {
                PingTask pingTask = new PingTask(id, getPingPacket(id),
                        this.nioTransport);

                pingTask.run(); // run once immediately
                ScheduledFuture<?> future = execpool
                        .scheduleAtFixedRate(pingTask,
                                (long) (PING_PERTURBATION_FACTOR
                                        * node_detection_timeout_millis * Math
                                        .random()),
                                inter_ping_period_millis,
                                TimeUnit.MILLISECONDS);
                futures.put(
                        id,
                        (ScheduledFuture<PingTask>) future);
            }
        } catch (JSONException e) {
            log.severe("Can not create ping packet at node " + this.myID
                    + " for node " + id);
            e.printStackTrace();
        }
        adjustFDParams(); // check to adjust every time sendKeepAlive is invoked
    }

    protected void receive(FailureDetectionPacket<NodeIDType> fdp) {
        log.log(Level.FINEST, "{0}{1}{2}", new Object[] {
                this.myID, " received ping from node ", fdp.senderNodeID });
        this.heardFrom(fdp.senderNodeID);
    }

    protected void heardFrom(NodeIDType id) {
        this.lastHeardFrom.put(id, System.currentTimeMillis());
    }

    protected boolean isNodeUp(NodeIDType id) {
        if (id == this.myID)
            return true;
        if (this.nioTransport.isDisconnected(id))
            return false;
        return ((System.currentTimeMillis() - lastHeardTime(id)) < node_detection_timeout_millis);
    }


    // don't synchronize; invoked in log messages
    protected long getDeadTime(NodeIDType id) {
        return (System.currentTimeMillis() - lastHeardTime(id));
    }

    private long lastHeardTime(NodeIDType id) {
        this.lastHeardFrom.putIfAbsent(id, initTime);
        return this.lastHeardFrom.get(id);
    }

    private JSONObject getPingPacket(NodeIDType id) throws JSONException {
        FailureDetectionPacket<NodeIDType> fdp = new FailureDetectionPacket<NodeIDType>(
                myID, id, true);
        JSONObject fdpJson = fdp.toJSONObject();
        return fdpJson;
    }

    private class PingTask implements Runnable {
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
                if(!FailureDetection.this.execpool.isShutdown())
                    nioTransport.sendToID(destID, pingJson);
            } catch (IOException e) {
                try {
                    log.log(Level.INFO,
                            "{0} encountered IOException while sending keepalive to {2}",
                            new Object[] { pingJson.getInt("sender"), destID });
                    cleanupFailedPingTask(destID);
                } catch (JSONException je) {
                    e.printStackTrace();
                }
            }
        }
    }

    private synchronized void cleanupFailedPingTask(NodeIDType id) {
        ScheduledFuture<PingTask> pingTask = this.futures.get(id);
        if (pingTask != null) {
            pingTask.cancel(true);
            this.futures.remove(id);
            sendKeepAlive(id);
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        System.out
                .println("FAILURE: I am not testable. Try running PaxosManager's test for now.");
    }
}

