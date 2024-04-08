package edu.umass.cs.consistency.MonotonicReads;

import edu.umass.cs.consistency.EventualConsistency.DynamoManager;
import edu.umass.cs.gigapaxos.interfaces.Replicable;

import java.util.ArrayList;
import java.util.Set;

public class MRReplicatedStateMachine {
    private ArrayList<Integer> members;
    private final String serviceName;
    private final int version;
    private MRManager<?> mrManager = null;
    public MRReplicatedStateMachine(String serviceName, int version, int id,
                                        Set<Integer> members, Replicable app, String initialState,
                                    MRManager<?> mrManager){
        this.members = new ArrayList<Integer>(members);
        this.serviceName = serviceName;
        this.version = version;
        this.mrManager = mrManager;

//        restore yet to be implemented

    }
    @Override
    public String toString(){
        StringBuilder members = new StringBuilder("[");
        for (int member : this.members) {
            members.append(member).append(",");
        }
        members.append("]");
        return "("+this.serviceName+","+this.version+","+members.toString()+")";
    }
    public ArrayList<Integer> getMembers() {
        return this.members;
    }
    public int[] getMembersArray() {
        int[] membersArray = new int[this.members.size()];
        for (int i = 0; i < this.members.size(); i++) {
            membersArray[i] = this.members.get(i);
        }
        return membersArray;
    }
    public int getVersion() {
        return this.version;
    }

    public String getServiceName() {
        return serviceName;
    }
}
