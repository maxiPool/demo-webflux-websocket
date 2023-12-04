package dev.maxipool.demowebfluxwebsockethtmx.consumer;

import dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC;
import dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.PublisherFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.LocalTime;

@Component
public class Consumer {

  private final Flux<OHLC> publisher;

  private Sinks.Many<OHLC> consumerInCallback;
  private Flux<OHLC> asFlux;

  public Consumer(PublisherFactory factory) {
    publisher = factory.createPublisher();
    consumePublisherAndEmitToWebSocketAndMakeAvailableToRestClients();
  }

  /**
   * What is the maximum rate at which I can emit values?
   * - From a single publisher (which can emit many items with different ids)
   * - From 5 publishers (one per source, each source can emit many items with different ids)
   */
  void consumePublisherAndEmitToWebSocketAndMakeAvailableToRestClients() {
    // we are normally just going to get a callback and not a flux.

    consumerInCallback = Sinks
        .many()
        .replay()
        .latest()
    ;
    asFlux = consumerInCallback
        .asFlux() // read-only version of a Sink
        .scan(OHLC::mergeUpdates)
        .sample(Duration.ofSeconds(1))
    ;

    publisher.subscribe(i -> {
      consumerInCallback.tryEmitNext(i);
      if (i.index() > 0 && i.index() % 100_000 == 0) {
        System.out.println("100k emissions from publisher");
      }
    });

    asFlux.subscribe(i -> System.out.println(LocalTime.now() + " asFlux sub: " + i));
  }

}
