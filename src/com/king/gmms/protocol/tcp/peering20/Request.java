package com.king.gmms.protocol.tcp.peering20;

public abstract class Request extends Pdu {
    /**
     * Request constructor comment.
     *
     * @param version int
     */
    public Request() {
    }

    public boolean canRespond() {
        return true;
    }

    protected abstract Respond createResponse();

    public Respond getRespond() {
        Respond response = createResponse();
//        response.getHeader().setSequenceId(this.getHeader().getSequenceId());
        return response;
    }

//    private synchronized long nextSequenceNum() {
//        sequenceNumber++;
//        if (sequenceNumber > 4294967295L)
//            sequenceNumber = 1;
//        return sequenceNumber;
//    }

    public boolean isRequest() {
        return true;
    }

}
