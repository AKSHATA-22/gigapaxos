package edu.umass.cs.gigapaxos.interfaces;

public interface Reconcilable extends Replicable {
    public boolean execute(Request request, boolean doNotReplyToClient);
    public String checkpoint(String name);
    public boolean restore(String name, String state);
    public Request reconcile(Request[] requests);
}
