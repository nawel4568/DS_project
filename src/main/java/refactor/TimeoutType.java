package refactor;

public enum TimeoutType {
    UPDATE(700),
    WRITEOK(1000),
    ELECTION_ACK(1000),
    ELECTION_PROTOCOL(5000),
    RECEIVE_HEARTBEAT(1500),
    SEND_HEARTBEAT(500);

    private final int millis;
    TimeoutType(int millis){
        this.millis=millis;
    }

    public int getMillis(){ return this.millis; }
}
