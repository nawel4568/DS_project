package refactor.Messages;

import java.io.Serializable;
import java.util.Queue;

public abstract class CoordinatorMessages implements Serializable {

    public static class HeartbeatMsg extends CoordinatorMessages {}

    public static class UpdateMsg extends CoordinatorMessages {
        public final Timestamp timestamp;
        public final Data value;
        public UpdateMsg(Timestamp timestamp, Data value){
            this.timestamp = timestamp;
            this.value = value;
        }
    }

    public static class WriteOKMsg extends CoordinatorMessages {
        public final Timestamp timestamp;
        public WriteOKMsg(Timestamp timestamp) {
            this.timestamp = timestamp;
        }
    }

    public static class SyncMsg extends CoordinatorMessages {
        //public final Queue<Snapshot> sync;
        //public SyncMsg(Queue<Snapshot> syncHistory) {
            //this.sync = syncHistory;
        //}
    }

    public static class Heartbeat extends CoordinatorMessages{}
}
