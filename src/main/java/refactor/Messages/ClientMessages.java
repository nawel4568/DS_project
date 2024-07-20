package refactor.Messages;

import java.io.Serializable;

public abstract class ClientMessages implements Serializable {

    public static class ReadReqMsg extends ClientMessages{}

    public static class WriteReqMsg extends ClientMessages {
        private final int value;
        public WriteReqMsg(int value) {
            this.value = value;
        }
        public int getValue() {
            return value;
        }
    }

    /*
    Triggers to send to the client from the main
    */
    public static class TriggerReadOperation extends ClientMessages{
        public final int targetReplica;
        public TriggerReadOperation(int targetReplica){
            this.targetReplica = targetReplica;
        }
    }

    public static class TriggerWriteOperation extends ClientMessages{
        public final int targetReplica;
        public final int value;
        public TriggerWriteOperation(int targetReplica, int value){
            this.targetReplica = targetReplica;
            this.value = value;
        }
    }
}
