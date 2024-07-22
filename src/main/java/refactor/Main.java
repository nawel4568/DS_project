package refactor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import refactor.Messages.ClientMessages;
import refactor.Messages.ReplicaMessages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    final static int N_REPLICAS = 6;
    Random rand = new Random();


    public static void main(String[] args) {

        Utils.clearFileContent("log.txt");

        final ActorSystem system = ActorSystem.create("DistributedSystem");

        // Create the Group of Replicas
        List<ActorRef> group = new ArrayList<ActorRef>();
        for (int i=0; i<N_REPLICAS; i++)
            group.add(system.actorOf(refactor.Replica.props(i), "Replica" + i));

        // ensure that no one can modify the group
        group = Collections.unmodifiableList(group);

        // Send the Start message to all the Replicas
        //ReplicaMessages.StartMsg start = new ReplicaMessages.StartMsg(group);

        for (ActorRef peer: group) {
            peer.tell(new ReplicaMessages.StartMsg(group), ActorRef.noSender());
        }
        sleep(5000);
        inputContinue();

        ActorRef client1 = system.actorOf(Client.props(1),"Client1");
        client1.tell(new ReplicaMessages.StartMsg(group), ActorRef.noSender());

        sleep(1000);
        inputContinue();

        client1.tell(new ClientMessages.TriggerWriteOperation(3,4), ActorRef.noSender());
        sleep(1000);
        inputContinue();

        client1.tell(new ClientMessages.TriggerWriteOperation(2,8), ActorRef.noSender());

        sleep(1000);
        inputContinue();

        client1.tell(new ClientMessages.TriggerReadOperation(3),ActorRef.noSender());
        sleep(1000);
        inputContinue();
        client1.tell(new ClientMessages.TriggerWriteOperation(0,7), ActorRef.noSender());
        sleep(3000);
        inputContinue();

        client1.tell(new ClientMessages.TriggerWriteOperation(4,9), ActorRef.noSender());

        sleep(1000);
        inputContinue();
        client1.tell(new ClientMessages.TriggerReadOperation(4),ActorRef.noSender());
        sleep(1000);

        inputContinue();
        system.terminate();


    }

    public static void inputContinue() {
        try {
            System.out.println(">>> Press ENTER to continue <<<");
            System.in.read();
        }
        catch (IOException ignored) {}
    }

    public static void sleep(int time) {
        try {
            Thread.sleep(time);
        }
        catch (Exception ignored){}
    }
}

