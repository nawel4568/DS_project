package refactor;

public enum TimeoutType {
    UPDATE(1000),
    WRITEOK(5000),
    ELECTION_ACK(500),
    ELECTION_PROTOCOL(5000),
    RECEIVE_HEARTBEAT(5000),
    SEND_HEARTBEAT(1000);

    private final int millis;
    TimeoutType(int millis){
        this.millis=millis;
    }

    public int getMillis(){ return this.millis; }
}
