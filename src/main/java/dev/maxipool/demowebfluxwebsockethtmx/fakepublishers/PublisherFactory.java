package dev.maxipool.demowebfluxwebsockethtmx.fakepublishers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.tools.agent.ReactorDebugAgent;

import java.time.Duration;
import java.util.Random;

import static dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC.createOHLC;
import static dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC.getNextOHLC;

@Slf4j
@Component
public class PublisherFactory {

  private final Random random = new Random();

  /**
   * A publisher of OHLC, can be subscribed to from many subscribers, shares latest value, emits each item only once.
   */
  public ConnectableFlux<OHLC> createPublisher() {
    return Flux
        .<OHLC, OHLC>generate(
            () -> createOHLC(random),
            (state, sink) -> {
              var nextOhlc = getNextOHLC(state, random);
              sink.next(state);
              return nextOhlc;
            })
        .zipWith(
            Flux.interval(Duration.ofNanos(10_000)), // 10k nanos = 100k emit per second
            (ohlc, interval) -> ohlc)
        .publish();
  }

  public static void main(String[] args) throws InterruptedException {
    ReactorDebugAgent.init();
    ReactorDebugAgent.processExistingClasses();

    var publisherFactory = new PublisherFactory();
    var pub = publisherFactory.createPublisher();

    pub.subscribe(i -> System.out.println("1st: " + i));
    Thread.sleep(5000);
    pub.subscribe(i -> System.out.println("2nd: " + i));

    Thread.sleep(10000);
  }

}
