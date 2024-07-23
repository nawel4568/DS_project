package refactor;

public enum TimeoutType {
    UPDATE(100),
    WRITEOK(2000),
    ELECTION_ACK(200),
    ELECTION_PROTOCOL(5000),
    RECEIVE_HEARTBEAT(5000),
    SEND_HEARTBEAT(500);

    private final int millis;
    TimeoutType(int millis){
        this.millis=millis;
    }

    public int getMillis(){ return this.millis; }
}
