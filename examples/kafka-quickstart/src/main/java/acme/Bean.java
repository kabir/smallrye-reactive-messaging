/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2020, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package acme;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

/**
 * @author <a href="mailto:kabir.khan@jboss.com">Kabir Khan</a>
 */
@ApplicationScoped
public class Bean {
    private final CountDownLatch latch = new CountDownLatch(3);

    public CountDownLatch getLatch() {
        return latch;
    }

    // Ugly workaround in WildFly TS where we do not expose RxJava or Mutiny to users
    /*
    @Outgoing("to-kafka")
    public PublisherBuilder<String> source() {
        // Does not work when backed by Kafka
        // (see https://github.com/smallrye/smallrye-reactive-messaging/issues/845)
        // return ReactiveStreams.of("hello", "reactive", "messaging");

        // Try something else
        System.out.println("====> Initialising stream!");
        String[] values = new String[] { "hello", "reactive", "messaging" };
        AtomicInteger count = new AtomicInteger(0);
        PublisherBuilder<String> pb =  ReactiveStreams.generate(() -> {
            try {
                Thread.sleep(3000);
                int curr = count.getAndIncrement();
                if (curr < values.length) {
                    String value = values[curr];
                    System.out.println("----> Sending " + value);
                    return value;
                }
            } catch (InterruptedException e) {
            }
            System.out.println("----> Sending null");
            return null;
        });
    }*/

    @Outgoing("to-kafka")
    public PublisherBuilder<String> source() {
        System.out.println("====> Initialising stream!");
        return ReactiveStreams.of("hello", "reactive", "messaging");
    }

    @Incoming("from-kafka")
    public void store(String payload) {
        System.out.println("-----> Received: " + payload);
    }
}
