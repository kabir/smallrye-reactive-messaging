package acme;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Broadcast;

@ApplicationScoped
public class Main {

    public static void main(String[] args) throws Exception {
        SeContainer container = SeContainerInitializer.newInstance().initialize();
        container.getBeanManager().createInstance().select(Main.class).get().consume();
        container.close();
    }

    @Broadcast
    @Outgoing("source")
    public PublisherBuilder<String> source() {
        return ReactiveStreams.of("hello", "with", "SmallRye", "reactive", "message");
    }

    // 1
    @Incoming("source")
    public void sink(String word) {
        System.out.println("Incoming >> " + word);
    }

    // 2
    @Incoming("source")
    public void sink2(String word) {
        System.out.println("Incoming(2) >> " + word);
    }

    // 3
    @Inject
    @Channel("source")
    Flowable<Message<String>> sourceStream;

    public List<String> consume() {
        return Flowable.fromPublisher(sourceStream)
                .map(Message::getPayload)
                .map(m -> {
                    System.out.println("Incoming >> " + m);
                    return m;
                })
                .toList()
                .blockingGet();
    }

}
