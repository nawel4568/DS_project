package refactor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import refactor.Messages.ClientMessages;
import refactor.Messages.CoordinatorMessages;
import refactor.Messages.ReplicaMessages;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Client extends AbstractActor {
    private final Utils.FileAdd file = new Utils.FileAdd("SystemLog.txt");
    private final Utils.FileAdd clientLog;
    private final int clientId;
    private int lastRead;
    private final List<ActorRef> replicas;

    public static Props props(int clientId, String clientLog) {
        return Props.create(refactor.Client.class, () -> new refactor.Client(clientId, clientLog));
    }

    public Client(int clientId, String clientLog) {
        this.clientId = clientId;
        this.replicas = new ArrayList<ActorRef>();
        this.clientLog = new Utils.FileAdd(clientLog);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReplicaMessages.StartMsg.class, this::onStartMsg)
                .match(Integer.class, this::onReceiveValue)
                .match(ClientMessages.TriggerReadOperation.class,  this::onTriggerReadOperation)
                .match(ClientMessages.TriggerWriteOperation.class, this::onTriggerWriteOperation)
                .match(ClientMessages.ReadScheduleMsg.class, this::onReadScheduleMsg)
                .build();
    }

    private void onStartMsg(ReplicaMessages.StartMsg msg){
        this.replicas.addAll(msg.group);
    }

    private void onReadScheduleMsg(ClientMessages.ReadScheduleMsg msg){
        Cancellable timeout = this.getContext().getSystem().scheduler().scheduleAtFixedRate(
                Duration.ZERO,
                Duration.ofMillis(msg.timing),
                this.replicas.get(msg.replicaID),
                new ClientMessages.ReadReqMsg(),
                getContext().system().dispatcher(),
                this.getSelf()
        );
    }

    private void onTriggerReadOperation(ClientMessages.TriggerReadOperation trigger){
        file.appendToFile(getSelf().path().name()+" read req to "+trigger.targetReplica);
        if(trigger.targetReplica >= 0 && trigger.targetReplica < this.replicas.size())
            this.replicas.get(trigger.targetReplica).tell(new ClientMessages.ReadReqMsg(), this.getSelf());
    }

    private void onTriggerWriteOperation(ClientMessages.TriggerWriteOperation trigger){
        if(trigger.targetReplica >= 0 && trigger.targetReplica < this.replicas.size())
            this.replicas.get(trigger.targetReplica).tell(new ClientMessages.WriteReqMsg(trigger.value), this.getSelf());

    }

    private void onReceiveValue(Integer val){
        file.appendToFile(getSelf().path().name()+" read done "+ val);
        clientLog.appendToFile(getSelf().path().name() + " reads " + val + " from " + getSender().path().name());
        if(!Objects.equals(this.getSelf().path().name(), "SeqConClient")){
            System.out.println("\nClient " + getSelf().path().name() + " received value " + val + " from " + getSender().path().name() + "\n");
            System.out.flush();
        }
    }

}
