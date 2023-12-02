package dev.maxipool.demowebfluxwebsockethtmx.fakepublishers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC.createOHLC;
import static dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC.getNextOHLC;

@Slf4j
@Component
public class PublisherFactory {

  private final Random random = new Random();

  private final ConcurrentMap<Integer, Flux<OHLC>> publishersMap = new ConcurrentHashMap<>();

  Flux<OHLC> createPublisher() {
    return Flux
        .<OHLC, OHLC>generate(
            () -> createOHLC(random),
            (state, sink) -> {
              var nextOhlc = getNextOHLC(state, random);
              sink.next(state);
              return nextOhlc;
            })
        .zipWith(
            Flux.interval(Duration.ofMillis(0), Duration.ofMillis(10000)),
            (ohlc, interval) -> ohlc)
        .doOnNext(i -> System.out.println("Emit #" + i.index()))
        .replay(1) // Flux --> ConnectableFlux but also it replays the latest emitted value for subscribers that come in late.
//        .publish() // turn the cold flux into a hot flux (Flux --> ConnectableFlux); CANNOT be combined with `replay()`
        .autoConnect() // connects to this hot flux when the 1st subscriber subscribes.
        ;
  }

  void createPublishers() {
    for (int i = 0; i < 10; i++) {
      publishersMap.put(i, createPublisher());
    }
  }

  public static void main(String[] args) throws InterruptedException {
    var publisherFactory = new PublisherFactory();
    var pub = publisherFactory.createPublisher();

    pub.subscribe(i -> System.out.println("1st: " + i));
    Thread.sleep(5000);
    pub.subscribe(i -> System.out.println("2nd: " + i));

    Thread.sleep(10000);
  }

}
