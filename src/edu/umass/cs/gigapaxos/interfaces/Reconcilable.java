package edu.umass.cs.gigapaxos.interfaces;

import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;

import java.util.ArrayList;

public interface Reconcilable extends Replicable {
//    public boolean execute(Request request, boolean doNotReplyToClient);
//    public String checkpoint(String name);
//    public boolean restore(String name, String state);

    /**
     *
     * @param requests
     * @return
     */

    public GraphNode reconcile(ArrayList<GraphNode> requests);
    public String stateForReconcile();
}
