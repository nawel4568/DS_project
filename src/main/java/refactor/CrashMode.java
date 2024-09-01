package refactor;

public class CrashMode {

    public enum CrashType {
        NO_CRASH,
        INSTANT_CRASH, //for crashing the coordinator immediately
        DURING_UPDATE_BROADCAST, //coordinator crashes while is sending the unstable update to the replicas
        BEFORE_WRITEREQ, //coordinator crashes before receiveing the write request (aka a client sends the write request but the coordinator is crashed)
        CAND_COORD_BEFORE_ACK, //the best andidate coordinator crashes during election, before becoming coordinator (token recirculates forever)
        CAND_COORD_AFTER_ACK; //the best candidate coordinator crashes after sending the ack to the predecessor but before becoming cooridinator (the token is lost and the replicas timout for the elction)
    }

    public final CrashType type;
    public final int param; //parameter used to set at what point of a broadcast the replica crashes

    public CrashMode(CrashType type, int param){
        this.type =  type;
        this.param = param;
    }

    @Override
    public String toString() {
        return "CrashMode{" +
                "type=" + type +
                ", param=" + param +
                '}';
    }

}