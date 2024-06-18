package org.example;

import akka.actor.ActorRef;

import java.io.Serializable;

public class Messages {
    public static class ReadRequest implements Serializable {
        public final ActorRef client;

        public ReadRequest(ActorRef client) {
            this.client = client;
        }
    }
}
