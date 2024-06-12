package edu.umass.cs.gigapaxos.interfaces;

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

    public Request reconcile(ArrayList<Request> requests);
    public String stateForReconcile();
}
