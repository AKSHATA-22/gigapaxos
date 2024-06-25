package edu.umass.cs.consistency.EventualConsistency.Domain;

public class RequestInformation {
    private final long requestID;
    private final String requestQuery;

    public RequestInformation(long requestID, String requestQuery) {
        this.requestID = requestID;
        this.requestQuery = requestQuery;
    }

    public String getRequestQuery() {
        return requestQuery;
    }

    public long getRequestID() {
        return requestID;
    }
}
