package refactor.Messages;

import java.io.Serializable;

public abstract class CoordinatorMessages implements Serializable {

    public static class HeartbeatMsg extends CoordinatorMessages {}
    public static class SyncMsg extends CoordinatorMessages {}

    public static class UpdateMsg extends CoordinatorMessages {
        public final Timestamp timestamp;
        public final Data data;
        public UpdateMsg(Timestamp timestamp, Data data){
            this.timestamp = new Timestamp(timestamp);
            this.data = new Data(data);
        }
    }

    public static class WriteOKMsg extends CoordinatorMessages {
        public final Timestamp timestamp;
        public WriteOKMsg(Timestamp timestamp) {
            this.timestamp = new Timestamp(timestamp);
        }
    }
}
