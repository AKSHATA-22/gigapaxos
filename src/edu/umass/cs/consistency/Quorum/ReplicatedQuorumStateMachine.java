package edu.umass.cs.consistency.Quorum;

import edu.umass.cs.gigapaxos.interfaces.Replicable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ReplicatedQuorumStateMachine {
    private ArrayList<Integer> quorumMembers; // from head to tail
    private final int readQuorum;
    private final int writeQuorum;

    private final String quorumID;
    private final int version;
    private final QuorumManager<?> quorumManager;

    public ReplicatedQuorumStateMachine(String quorumID, int version, int id,
                                        Set<Integer> members, Replicable app, String initialState,
                                        QuorumManager<?> qm){
        this.quorumMembers = (ArrayList<Integer>) List.copyOf(members);
        this.quorumID = quorumID;
        this.version = version;
        this.quorumManager = qm;
        if(!this.quorumMembers.contains(id)){
            this.quorumMembers.add(id);
        }
        if (this.quorumMembers.size()%2 == 0){
            this.readQuorum = (this.quorumMembers.size())/2+1;
            this.writeQuorum = (this.quorumMembers.size())/2;
        }
        else {
            this.readQuorum = (this.quorumMembers.size()+1)/2;
            this.writeQuorum = (this.quorumMembers.size()+1)/2;
        }
//        restore yet to be implemented

    }
    public ArrayList<Integer> getQuorumMembers() {
        return this.quorumMembers;
    }

    public int getReadQuorum() {
        return this.readQuorum;
    }

    public int getWriteQuorum() {
        return this.writeQuorum;
    }

    public String getQuorumID() {
        return this.quorumID;
    }

    public int getVersion() {
        return this.version;
    }
    @Override
    public String toString(){
        StringBuilder members = new StringBuilder("[");
        for (int quorumMember : this.quorumMembers) {
            members.append(quorumMember).append(",");
        }
        members.append("]");

        return "("+this.quorumID+","+this.version+","+members.toString()+",read quorum="
                +this.readQuorum+",write quorum="+this.writeQuorum+")";
    }
}
