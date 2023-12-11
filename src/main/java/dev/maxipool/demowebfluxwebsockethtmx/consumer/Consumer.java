package dev.maxipool.demowebfluxwebsockethtmx.consumer;

import dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC;
import dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.PublisherFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Optional.ofNullable;

@Slf4j
@Getter
@Component
public class Consumer {

  private final PublisherFactory factory;
  private Disposable version1Subscription;
  private ConnectableFlux<OHLC> version1Pub;

  public Consumer(PublisherFactory factory) {
    this.factory = factory;
    getVersion1Publisher(factory);
  }

  private void getVersion1Publisher(PublisherFactory factory) {
    version1Pub = factory.createPublisher();
    version1Subscription = version1Pub.subscribe(
        i -> sinksMap
            .compute(
                i.id(),
                (k, maybeV) -> ofNullable(maybeV)
                    .map(v -> {
                      v.sink().tryEmitNext(i);
                      return v;
                    })
                    .orElseGet(() -> {
                      var sf = getSinkFlux(i.id());
                      sf.sink().tryEmitNext(i);
                      return sf;
                    })
            ));
    version1Pub.connect();
  }

  /**
   * What is the maximum rate at which I can emit values?
   * - From a single publisher with a single item and single id.
   * - From a single publisher (which can emit many items with different ids)
   * - From 5 publishers (one per source, each source can emit many items with different ids)
   */
  void consumeSingleIdSingleSource() {
    // we are normally just going to get a callback and not a flux.
    var consumerInCallback = Sinks
        .many()
        .unicast()
        .<OHLC>onBackpressureBuffer();
    var asFlux = consumerInCallback
        .asFlux() // read-only version of a Sink
        .scan(OHLC::mergeUpdates)
        .sample(Duration.ofSeconds(1));

    var publisher = factory.createPublisher();
    publisher.subscribe(i -> {
      consumerInCallback.tryEmitNext(i);
      if (i.index() > 0 && i.index() % 100_000 == 0) {
        System.out.println("100k emissions from publisher");
      }
    });

    asFlux.subscribe(i -> System.out.println(LocalTime.now() + " asFlux sub: " + i));
  }

  private final ConcurrentMap<Integer, SinkFlux> sinksMap = new ConcurrentHashMap<>((int) (1.5 * 1024));

  private record SinkFlux(Integer id, Sinks.Many<OHLC> sink, Flux<OHLC> flux) {
  }

  /**
   * The issue with this version is the following:
   * <br />
   * I will have 1000+ Sink/Flux
   * <br />
   * each time I get an update, in the callback, I must tryEmitNext to the correct Sink
   * <br />
   * This means I have to retrieve the Sink in a Map, then tryEmitNext
   * <br />
   * Every second, the Flux will emit the merged value for this item id,
   * which will have to be merged with the previously emitted value. --> scan operator
   * <br /><br />
   * So I need a {@code Map<item_id, Sink>}
   */

  private static SinkFlux getSinkFlux(Integer id) {
    var sink = Sinks
        .many()
        .replay()
        .<OHLC>latest();

    var splitMeFlux = sink
        .asFlux()
        .publish();

    var head = splitMeFlux.next().log();
    var sampledTail = splitMeFlux
        .skip(1)
        .scan(OHLC::mergeUpdates)
        .sample(Duration.ofSeconds(1));
    splitMeFlux.connect();

    var finalFlux = sampledTail
        .startWith(head)
        .share()
        .replay(1);
    finalFlux.connect();

    return new SinkFlux(id, sink, finalFlux);
  }

  public Flux<OHLC> getByIdVersion1(Integer id) {
    return sinksMap.get(id).flux();
  }

  //  public ConnectableFlux<Map<String, ConnectableFlux<OHLC>>> get
  public Collection<Flux<OHLC>> getAllFlux() {
    return sinksMap.values().stream().map(SinkFlux::flux).toList();
  }

  /**
   * Actually, there's another way to do the sampling/merging: I can use a {@code Map<item_id, Sink>} inside the
   * sampler and sample all the different items in a single sampler.
   * <br />
   * Then I emit this Map's values and merge them with the previously saved values.
   */
  void consumeManyIdSingleSourceVersion2() {

  }

}
