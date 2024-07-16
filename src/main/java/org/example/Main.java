package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;
import java.util.Random;



import static org.example.Utils.DEBUG;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    final static int N_REPLICAS = 6;
    Random rand = new Random();


    public static void main(String[] args) {

        Utils.clearFileContent("output.txt");

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
        if(DEBUG){
            System.out.println("Entering the loop for sending the start messages...");
            System.out.flush();
        }

        for (ActorRef peer: group) {
            peer.tell(start, ActorRef.noSender());
            try {
                Thread.sleep((50));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        inputContinue();
        /*try {
            Thread.sleep((1000));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } */
        //Arbitrarily pick a replica for initializing the value
        group.get(1).tell(new Messages.WriteReqMsg(-1), ActorRef.noSender()); // **** Write

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        //inputContinue();


        // Create the Clients
        ActorRef client1 = system.actorOf(Client.props(1),"Client1");
        ActorRef client2 = system.actorOf(Client.props(2),"Client2");
        ActorRef client3 = system.actorOf(Client.props(3),"Client3");



        group.get(2).tell(new Messages.WriteReqMsg(5), client1); // **** Write
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //inputContinue();

        group.get(3).tell(new Messages.ReadReqMsg(), client1); // **** Read
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //inputContinue();

        group.get(3).tell(new Messages.WriteReqMsg(4), client1); // **** Write`
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //inputContinue();

        group.get(3).tell(new Messages.WriteReqMsg(9), client1); // **** Write
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //inputContinue();

        group.get(3).tell(new Messages.ReadReqMsg(), client1); // **** Read
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //inputContinue();
        group.get(3).tell(new Messages.ReadReqMsg(), client1);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        inputContinue();

        /** group.get(2).tell(new Messages.WriteReqMsg(8), client3); // **** Write
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //inputContinue();

        group.get(3).tell(new Messages.ReadReqMsg(), client1); // **** Read
        //inputContinue();

        group.get(5).tell(new Messages.WriteReqMsg(1), client1); // **** Write
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //inputContinue();

        group.get(0).tell(new Messages.ReadReqMsg(), client3); // **** Read
        //inputContinue();

        group.get(4).tell(new Messages.ReadReqMsg(), client2); // **** Read
        //inputContinue();

        group.get(1).tell(new Messages.WriteReqMsg(6), client2); // **** Write
        //inputContinue();**/

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

