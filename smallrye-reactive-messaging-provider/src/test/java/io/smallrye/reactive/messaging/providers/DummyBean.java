package io.smallrye.reactive.messaging.providers;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DummyBean {

  @Incoming(topic = "dummy-source", provider = Dummy.class)
  @Outgoing(topic = "dummy-sink", provider = Dummy.class)
  public ProcessorBuilder<Integer, String> process() {
    return ReactiveStreams.<Integer>builder().map(i -> i * 2).map(i -> Integer.toString(i));
  }

}