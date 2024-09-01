package refactor.Messages;

import refactor.CrashMode;

import java.io.Serializable;

public abstract class DebugMessages implements Serializable {
    public static class CrashMsg extends DebugMessages {
        public CrashMode mode;
        public CrashMsg(CrashMode mode){
            this.mode = mode;
        }
    }
}
