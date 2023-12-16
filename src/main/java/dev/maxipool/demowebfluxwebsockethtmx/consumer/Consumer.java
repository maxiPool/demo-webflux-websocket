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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
//    getVersion1Publisher(factory);

    getVersion2Publishers(factory);
  }

  /**
   * Uses a single publisher (which can emit many items with different ids)
   */
  private void getVersion1Publisher(PublisherFactory factory) {
    final var publisherId = 0;
    version1Pub = factory.createPublisher(0, 100_000, 512);
    version1Subscription = version1Pub.subscribe(i -> putInSinksMap(publisherId, i));
    version1Pub.connect();
  }

  private final Map<Integer, Disposable> version2Subscriptions = new HashMap<>();
  private Map<Integer, PublisherFactory.PublisherInfo> version2Pubs;

  /**
   * Uses 5 publishers
   */
  private void getVersion2Publishers(PublisherFactory factory) {
    version2Pubs = factory.createPublishers(5, 100_000, 512);

    version2Pubs
        .forEach((publisherId, publisherInfo) -> {
          var disposable = publisherInfo
              .publisher()
              .subscribe(i -> putInSinksMap(publisherId, i));
          version2Subscriptions.put(publisherId, disposable);
        });

    version2Pubs.values().forEach(v -> v.publisher().connect());
  }

  private SinkFlux putInSinksMap(Integer publisherId, OHLC i) {
    return sinksMap
        .compute(
            new SourceIdOhlcId(publisherId, i.id()),
            (k, maybeV) -> ofNullable(maybeV)
                .map(v -> {
                  v.sink().tryEmitNext(i);
                  return v;
                })
                .orElseGet(() -> {
                  var sf = getSinkFlux(publisherId, i.id());
                  sf.sink().tryEmitNext(i);
                  return sf;
                })
        );
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

    var publisher = factory.createPublisher(0, 100_000, 512);
    publisher.subscribe(i -> {
      consumerInCallback.tryEmitNext(i);
      if (i.index() > 0 && i.index() % 100_000 == 0) {
        System.out.printf("%dk emissions from publisher%n", i.index() / 1000);
      }
    });

    asFlux.subscribe(i -> System.out.println(LocalTime.now() + " asFlux sub: " + i));
  }

  public record SourceIdOhlcId(int sourceId, int ohlcId) {
  }

  public record FluxWithMetadata(SourceIdOhlcId id, Flux<OHLC> flux) {
  }

  private final ConcurrentMap<SourceIdOhlcId, SinkFlux> sinksMap = new ConcurrentHashMap<>((int) (1.5 * 1024));

  private record SinkFlux(SourceIdOhlcId id, Sinks.Many<OHLC> sink, Flux<OHLC> flux) {
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
  private static SinkFlux getSinkFlux(Integer publisherId, Integer ohlcId) {
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

    return new SinkFlux(new SourceIdOhlcId(publisherId, ohlcId), sink, finalFlux);
  }

  public Flux<OHLC> getByIdVersion1(Integer id) {
    return sinksMap.get(id).flux();
  }

  // TODO: to add/remove from the Map: Flux<Map<String, ConnectableFlux<OHLC>>>
  public List<FluxWithMetadata> getAllFlux() {
    return sinksMap
        .entrySet().stream()
        .map(i -> new FluxWithMetadata(i.getKey(), i.getValue().flux()))
        .toList();
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
