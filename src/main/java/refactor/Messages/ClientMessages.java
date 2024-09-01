package refactor.Messages;

import akka.actor.ActorPath;
import akka.actor.ActorRef;

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

    //Message for setting the read schedule to a client
    public static class ReadScheduleMsg extends ClientMessages{
        public final int timing;
        public final int replicaID;
        public ReadScheduleMsg(int timing, int replicaID){
            this.timing = timing;
            this.replicaID = replicaID;
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
