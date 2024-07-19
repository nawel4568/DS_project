package refactor.Messages;

import java.io.Serializable;

public abstract class ClientMessages implements Serializable {

    public static class ReadReqMsg extends ClientMessages{}

    public static class WriteReqMsg extends ReplicaMessages {
        private final int v;
        public WriteReqMsg(int v) {
            this.v = v;
        }
        public int getV() {
            return v;
        }
    }

}
