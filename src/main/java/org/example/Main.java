package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.FileWriter;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;
import java.util.Random;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    final static int N_REPLICAS = 6;
    Random rand = new Random();


    public static void main(String[] args) {



        final ActorSystem system = ActorSystem.create("DistributedSystem");


        // Create the Group of Replicas
        List<ActorRef> group = new ArrayList<ActorRef>();
        for (int i=0; i<N_REPLICAS; i++)
            group.add(system.actorOf(Replica.props(i), "Replica" + i));

        // ensure that no one can modify the group
        group = Collections.unmodifiableList(group);

        // Send the Start message to all the Replicas
        Messages.StartMessage start = new Messages.StartMessage(group);
        //group.get(0).tell(start, ActorRef.noSender());


        for (ActorRef peer: group) {
            peer.tell(start, ActorRef.noSender());

        }

        //inputContinue();


        // Create the Clients
        ActorRef client1 = system.actorOf(Client.props(1),"Client1");
        ActorRef client2 = system.actorOf(Client.props(2),"Client2");
        ActorRef client3 = system.actorOf(Client.props(3),"Client3");


        for(ActorRef peer: group){
            System.out.println("name: "+peer.path().name());
            System.out.println("uid: "+peer.path().uid());
        }

        /** problem is Coordinator == null **/
        group.get(3).tell(new Messages.WriteReqMsg(5), client1); // **** Write
        //inputContinue();

        group.get(2).tell(new Messages.WriteReqMsg(8), client3); // **** Write
        //inputContinue();

        group.get(3).tell(new Messages.ReadReqMsg(), client1); // **** Read
        //inputContinue();

        group.get(5).tell(new Messages.WriteReqMsg(1), client1); // **** Write
        //inputContinue();

        group.get(0).tell(new Messages.ReadReqMsg(), client3); // **** Read
        //inputContinue();

        group.get(4).tell(new Messages.ReadReqMsg(), client2); // **** Read
        //inputContinue();

        group.get(1).tell(new Messages.WriteReqMsg(6), client2); // **** Write
        //inputContinue();

        system.terminate();


    }

    public static void inputContinue() {
        try {
            System.out.println(">>> Press ENTER to continue <<<");
            System.in.read();
        }
        catch (IOException ignored) {}
    }
}

