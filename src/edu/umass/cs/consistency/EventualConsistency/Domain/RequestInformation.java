package edu.umass.cs.consistency.EventualConsistency.Domain;

/**
 * This class is used to summarize the important fields in a request, requestID and requestQuery
 */

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
