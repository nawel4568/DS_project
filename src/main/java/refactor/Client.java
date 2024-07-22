package refactor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import refactor.Messages.ClientMessages;
import refactor.Messages.ReplicaMessages;

import java.util.ArrayList;
import java.util.List;

public class Client extends AbstractActor {
    Utils.FileAdd file = new Utils.FileAdd("log.txt");

    private final int clientId;
    private int lastRead;
    private final List<ActorRef> replicas;

    public static Props props(int clientId) {
        return Props.create(refactor.Client.class, () -> new refactor.Client(clientId));
    }

    public Client(int clientId) {
        this.clientId = clientId;
        this.replicas = new ArrayList<ActorRef>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReplicaMessages.StartMsg.class, this::onStartMsg)
                .match(Integer.class, this::onReceiveValue)
                .match(ClientMessages.TriggerReadOperation.class,  this::onTriggerReadOperation)
                .match(ClientMessages.TriggerWriteOperation.class, this::onTriggerWriteOperation)
                .build();
    }

    private void onStartMsg(ReplicaMessages.StartMsg msg){
        this.replicas.addAll(msg.group);
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
        file.appendToFile(getSelf().path().name()+" read done "+val);
        System.out.println("\nClient "+getSelf().path().name() + " received value " + val+ " from "+getSender().path().name()+"\n");
        System.out.flush();
    }



}
