package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    final static int N_REPLICAS = 4;
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("DistributedSystem");

        List<ActorRef> group = new ArrayList<>();
        for (int i=0; i<N_REPLICAS; i++) {
            group.add(system.actorOf(Replica.props(i), "Replica" + i));
        }

    }
}