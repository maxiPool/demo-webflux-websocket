package dev.maxipool.demowebfluxwebsockethtmx.fakepublishers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.tools.agent.ReactorDebugAgent;

import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC.createOHLC;
import static dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC.getNextOHLC;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Component
public class PublisherFactory {

  private final Random random = new Random();

  private static final int NANOS_PER_SECOND = 1_000_000_000;

  /**
   * A publisher of OHLC, can be subscribed to from many subscribers, shares latest value, emits each item only once.
   */
  public ConnectableFlux<OHLC> createPublisher(double nbEmitPerSecond, int nbOfOhlcIds) {
    // 1 second = 1 000 000 000 nanos
    // 1 emit per second = 1 000 000 000 nanos
    // 1 000 000 emit per second = 1 000 nanos
    // 1 000 000 000 emit per second = 1 nanos
    var duration = Duration.ofNanos((long) (NANOS_PER_SECOND / nbEmitPerSecond));
    return Flux
        .<OHLC, OHLC>generate(
            () -> createOHLC(random),
            (state, sink) -> {
              var nextOhlc = getNextOHLC(state, random, nbOfOhlcIds);
              sink.next(state);
              return nextOhlc;
            })
        .zipWith(
            Flux.interval(duration),
            (ohlc, interval) -> ohlc)
        .publish();
  }

  public record PublisherInfo(int sourceId, ConnectableFlux<OHLC> publisher) {
  }

  public Map<Integer, PublisherInfo> createPublishers(int nbOfPublishersToCreate, double nbEmitPerSecond, int nbOfOhlcIds) {
    final var finalNbEmitPerSecond = nbEmitPerSecond / nbOfPublishersToCreate;
    ;
    return IntStream
        .range(0, nbOfPublishersToCreate)
        .mapToObj(i -> new PublisherInfo(i, createPublisher(finalNbEmitPerSecond, nbOfOhlcIds)))
        .collect(toMap(PublisherInfo::sourceId, i -> i));
  }

  public static void main(String[] args) throws InterruptedException {
    ReactorDebugAgent.init();
    ReactorDebugAgent.processExistingClasses();

    var publisherFactory = new PublisherFactory();
    var pub = publisherFactory.createPublisher(10, 4);
    var pubWithLog = pub.log();
    pub.connect();

    final var count1st = new AtomicInteger(0);
    final var count2nd = new AtomicInteger(0);

    pubWithLog.subscribe(i -> {
//      System.out.println("1st: " + i);
      count1st.incrementAndGet();
    });
    Thread.sleep(2000);
    pubWithLog.subscribe(i -> {
//      System.out.println("2nd: " + i);
      count2nd.incrementAndGet();
    });
    Thread.sleep(2000);

    System.out.printf("Counts: 1st=%d, 2nd=%d%n", count1st.get(), count2nd.get());
  }

}
