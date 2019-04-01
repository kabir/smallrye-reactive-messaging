package io.smallrye.reactive.messaging.extension;

import io.smallrye.reactive.messaging.*;
import io.smallrye.reactive.messaging.annotations.Merge;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.*;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Class responsible for managing mediators
 */
@ApplicationScoped
public class MediatorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MediatorManager.class);
  public static final String STRICT_MODE_PROPERTY = "smallrye-messaging-strict-binding";
  private final boolean strictMode;

  private final CollectedMediatorMetadata collected = new CollectedMediatorMetadata();

  // TODO Populate this list
  private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

  private final List<AbstractMediator> mediators = new ArrayList<>();

  @Inject
  @Any
  Instance<StreamRegistar> streamRegistars;

  @Inject
  MediatorFactory mediatorFactory;

  @Inject
  StreamRegistry streamRegistry;

  @Inject
  BeanManager beanManager;

  private boolean initialized;

  public MediatorManager() {
    strictMode = Boolean.parseBoolean(System.getProperty(STRICT_MODE_PROPERTY, "false"));
    if (strictMode) {
      LOGGER.debug("Strict mode enabled");
    }
  }

  public boolean isInitialized() {
    return initialized;
  }

  public <T> void analyze(AnnotatedType<T> annotatedType, Bean<T> bean) {
    LOGGER.info("Scanning Type: {}", annotatedType.getJavaClass());
    Set<AnnotatedMethod<? super T>> methods = annotatedType.getMethods();

    methods.stream()
      .filter(method -> method.isAnnotationPresent(Incoming.class) || method.isAnnotationPresent(Outgoing.class))
      .forEach(method -> collected.add(method.getJavaMember(), bean));
  }

  public <T> void analyze(Class<?> beanClass, Bean<T> bean) {
    LOGGER.info("Scanning type: {}", beanClass);
    Class<?> current = beanClass;
    while (current != Object.class) {
      Arrays.stream(current.getDeclaredMethods())
        .filter(m -> m.isAnnotationPresent(Incoming.class) || m.isAnnotationPresent(Outgoing.class))
        .forEach(m -> collected.add(m, bean));
      current = current.getSuperclass();
    }
  }

  @PreDestroy
  void shutdown() {
    LOGGER.info("Cancel subscriptions");
    subscriptions.forEach(Subscription::cancel);
    subscriptions.clear();
  }

  public void initializeAndRun() {
    if (initialized) {
      throw new IllegalStateException("MediatorManager was already initialized!");
    }
    LOGGER.info("Deployment done... start processing");

    streamRegistars.stream().forEach(StreamRegistar::initialize);
    Set<String> unmanagedSubscribers = streamRegistry.getOutgoingNames();
    LOGGER.info("Initializing mediators");
    collected.mediators()
      .forEach(configuration -> {

        AbstractMediator mediator = createMediator(configuration);

        LOGGER.debug("Initializing {}", mediator.getMethodAsString());

        if (configuration.getInvokerClass() != null) {
          try {
            mediator.setInvoker(configuration.getInvokerClass()
              .newInstance());
          } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.error("Unable to create invoker instance of " + configuration.getInvokerClass(), e);
            return;
          }
        }

        try {
          Object beanInstance = beanManager.getReference(configuration.getBean(), Object.class, beanManager.createCreationalContext(configuration.getBean()));
          mediator.initialize(beanInstance);
        } catch (Throwable e) {
          LOGGER.error("Unable to initialize mediator: " + mediator.getMethodAsString(), e);
          return;
        }

        if (mediator.getConfiguration()
          .shape() == Shape.PUBLISHER) {
          LOGGER.debug("Registering {} as publisher {}", mediator.getConfiguration()
              .methodAsString(),
            mediator.getConfiguration()
              .getOutgoing());
          streamRegistry.register(mediator.getConfiguration().getOutgoing(), mediator.getStream());
        }
        if (mediator.getConfiguration()
          .shape() == Shape.SUBSCRIBER) {
          LOGGER.debug("Registering {} as subscriber {}", mediator.getConfiguration()
              .methodAsString(),
            mediator.getConfiguration()
              .getIncoming());
          streamRegistry.register(mediator.getConfiguration().getIncoming(), mediator.getComputedSubscriber());
        }
      });

    try {
      weaving(unmanagedSubscribers);
    } catch (WeavingException e) {
      throw new DeploymentException(e);
    }
  }

  private void weaving(Set<String> unmanagedSubscribers) {
    // At that point all the publishers have been registered in the registry
    LOGGER.info("Connecting mediators");
    List<AbstractMediator> unsatisfied = getAllNonSatisfiedMediators();

    // This list contains the names of the streams that have bean connected and
    List<LazySource> lazy = new ArrayList<>();
    while (!unsatisfied.isEmpty()) {
      int numberOfUnsatisfiedBeforeLoop = unsatisfied.size();

      unsatisfied.forEach(mediator -> {
        LOGGER.info("Attempt to resolve {}", mediator.getMethodAsString());
        List<PublisherBuilder<? extends Message>> sources = streamRegistry.getPublishers(mediator.configuration().getIncoming());
        Optional<PublisherBuilder<? extends Message>> maybeSource = getAggregatedSource(sources, mediator, lazy);
        maybeSource.ifPresent(publisher -> {
          mediator.connectToUpstream(publisher);
          LOGGER.info("Connecting {} to `{}` ({})", mediator.getMethodAsString(), mediator.configuration()
            .getIncoming(), publisher);
          if (mediator.configuration()
            .getOutgoing() != null) {
            streamRegistry.register(mediator.getConfiguration().getOutgoing(), mediator.getStream());
          }
        });
      });

      unsatisfied = getAllNonSatisfiedMediators();
      int numberOfUnsatisfiedAfterLoop = unsatisfied.size();

      if (numberOfUnsatisfiedAfterLoop == numberOfUnsatisfiedBeforeLoop) {
        // Stale!
        if (strictMode) {
          throw new WeavingException("Impossible to bind mediators, some mediators are not connected: " + unsatisfied.stream()
            .map(m -> m.configuration()
              .methodAsString())
            .collect(Collectors.toList()) + ", available publishers:" + streamRegistry.getIncomingNames());
        } else {
          LOGGER.warn("Impossible to bind mediators, some mediators are not connected: {}", unsatisfied.stream()
            .map(m -> m.configuration()
              .methodAsString())
            .collect(Collectors.toList()));
          LOGGER.warn("Available publishers: {}", streamRegistry.getIncomingNames());
        }
        break;
      }
    }

    // Inject lazy sources
    lazy.forEach(l -> l.configure(streamRegistry, LOGGER));

    // Run
    mediators.stream()
      .filter(m -> m.configuration()
        .shape() == Shape.SUBSCRIBER)
      .filter(AbstractMediator::isConnected)
      .forEach(AbstractMediator::run);

    // We also need to connect mediator to un-managed subscribers
    for (String name : unmanagedSubscribers) {
      List<AbstractMediator> list = lookupForMediatorsWithMatchingDownstream(name);
      for (AbstractMediator mediator : list) {
        List<SubscriberBuilder<? extends Message, Void>> subscribers = streamRegistry.getSubscribers(name);

        if (subscribers.size() == 1) {
          LOGGER.info("Connecting method {} to sink {}", mediator.getMethodAsString(), name);
          mediator.getStream().to((SubscriberBuilder) subscribers.get(0)).run();
        } else if (subscribers.size() > 2) {
          LOGGER.warn("{} subscribers consuming the stream {}", subscribers.size(), name);
          subscribers.forEach(s -> {
            LOGGER.info("Connecting method {} to sink {}", mediator.getMethodAsString(), name);
            mediator.getStream().to((SubscriberBuilder) s).run();
          });
        }
      }
    }

    initialized = true;
  }

  private List<AbstractMediator> lookupForMediatorsWithMatchingDownstream(String name) {
    return mediators.stream()
      .filter(m -> m.configuration()
        .getOutgoing() != null)
      .filter(m -> m.configuration()
        .getOutgoing()
        .equalsIgnoreCase(name))
      .collect(Collectors.toList());
  }

  private List<AbstractMediator> getAllNonSatisfiedMediators() {
    return mediators.stream()
      .filter(mediator -> !mediator.isConnected())
      .collect(Collectors.toList());
  }

  private AbstractMediator createMediator(MediatorConfiguration configuration) {
    AbstractMediator mediator = mediatorFactory.create(configuration);
    LOGGER.debug("Mediator created for {}", configuration.methodAsString());
    mediators.add(mediator);
    return mediator;
  }

  private Optional<PublisherBuilder<? extends Message>> getAggregatedSource(List<PublisherBuilder<? extends Message>> sources, AbstractMediator mediator,
                                                                     List<LazySource> lazy) {
    if (sources.isEmpty()) {
      return Optional.empty();
    }

    Merge.Mode merge = mediator.getConfiguration()
      .getMerge();
    if (merge != null) {
      LazySource lazySource = new LazySource(mediator.configuration()
        .getIncoming(), merge);
      lazy.add(lazySource);
      return Optional.of(ReactiveStreams.fromPublisher(lazySource));
    }

    if (sources.size() > 1) {
      throw new WeavingException(mediator.configuration()
        .getIncoming(), mediator.getMethodAsString(), sources.size());
    }
    return Optional.of(sources.get(0));

  }

}