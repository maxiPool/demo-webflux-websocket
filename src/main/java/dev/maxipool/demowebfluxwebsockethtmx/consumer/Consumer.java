package dev.maxipool.demowebfluxwebsockethtmx.consumer;

import dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC;
import dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.PublisherFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.OHLC.mergeUpdates;
import static java.util.Optional.ofNullable;
import static reactor.core.publisher.Flux.combineLatest;
import static reactor.core.publisher.Flux.fromStream;

@Slf4j
@Getter
@Component
public class Consumer {

  private final ConcurrentMap<SourceIdOhlcId, SinkFlux> sinksMap = new ConcurrentHashMap<>((int) (1.5 * 1024));

  private final PublisherFactory factory;
  private Disposable version1Subscription;
  private ConnectableFlux<OHLC> version1Pub;

  public Consumer(PublisherFactory factory) {
    this.factory = factory;
//    enableVersion1Publisher(factory);
//    enableVersion2Publishers(factory);
    enableVersion3Publishers();
  }

  /**
   * Uses a single publisher (which can emit many items with different ids)
   */
  private void enableVersion1Publisher(PublisherFactory factory) {
    final var publisherId = 0;
    version1Pub = factory.createPublisher(0, 100_000, 512);
    version1Subscription = version1Pub.subscribe(this::putInSinksMap);
    version1Pub.connect();
  }

  /**
   * Uses 5 publishers
   */
  private void enableVersion2Publishers(PublisherFactory factory) {
    var version2Pubs = factory.createPublishers(5, 100_000, 512);

    version2Pubs.forEach((publisherId, publisherInfo) -> publisherInfo.publisher().subscribe(this::putInSinksMap));

    version2Pubs.values().forEach(v -> v.publisher().connect());
  }

  private void putInSinksMap(OHLC i) {
    sinksMap
        .compute(
            new SourceIdOhlcId(i.sourceId(), i.id()),
            (k, maybeV) -> ofNullable(maybeV)
                .map(v -> {
                  v.sink().tryEmitNext(i);
                  return v;
                })
                .orElseGet(() -> {
                  var sf = getSinkFlux(i.sourceId(), i.id());
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

  public record SinkFlux(SourceIdOhlcId id, Sinks.Many<OHLC> sink, Flux<OHLC> flux) {
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
        .scan(OHLC::mergeUpdates)
        .share()
        .replay(1);
    finalFlux.connect();

    return new SinkFlux(new SourceIdOhlcId(publisherId, ohlcId), sink, finalFlux);
  }

  public Flux<OHLC> getByIdVersion1(Integer id) {
    return sinksMap.get(id).flux();
  }

  // TODO: to add/remove from the Map: Flux<Map<String, ConnectableFlux<OHLC>>>
  public List<FluxWithMetadata> getAllFluxWithMetadata() {
    return sinksMap
        .entrySet().stream()
        .map(i -> new FluxWithMetadata(i.getKey(), i.getValue().flux()))
        .toList();
  }

  public List<Flux<OHLC>> getAllFlux() {
    return sinksMap
        .values().stream()
        .map(SinkFlux::flux)
        .toList();
  }

  @Getter
  private ConnectableFlux<ConcurrentMap<SourceIdOhlcId, SinkFlux>> fluxOfMap;

  /**
   * Actually, there's another way to do the sampling/merging:<br />
   * Every ohlc ID from a single source would be put into a Map for the sampling<br />
   * when the sampling timeout ends, this Map is emitted and merged (scan operator) with the accumulator Map.
   * <br />
   * Then I emit this Map's values and merge them with the previously saved values.
   * <br /><br />
   * I hypothesize this method will be less resource intensive because we avoid creating a single FLUX for each subject.
   * <br /><br />
   * NOTE: I think there's an hybrid way:
   * <br />
   * do the sampling like this, with a single flux, but after the sampling,<br />
   * I can plug the flux into a {@code Map<item_id, Sink>} with a Sink for each topic.
   */
  void enableVersion3Publishers() {
    var nbOfOhlcIds = 5;
    var initialCapacity = (int) (nbOfOhlcIds * 1.5) + 1;

    var publishers = factory.createPublishers(1, 100, nbOfOhlcIds);

    var fluxStream = publishers.values().stream().map(PublisherFactory.PublisherInfo::publisher);
    var oncePerSecondOhlcMap = fromStream(fluxStream)
        .flatMap(f -> f)
        .scan(new ConcurrentHashMap<>(initialCapacity), Consumer::addEntryToMap)
        .sample(Duration.ofSeconds(1));

    var mySinkMap = new ConcurrentHashMap<SourceIdOhlcId, SinkFlux>(initialCapacity);
    fluxOfMap = combineLatest(
        Mono.just(mySinkMap),
        oncePerSecondOhlcMap,
        (ConcurrentMap<SourceIdOhlcId, SinkFlux> accumulatorMap, ConcurrentMap<SourceIdOhlcId, OHLC> oncePerSecondSamplingMap) -> {
          oncePerSecondSamplingMap.forEach((id, sampledOhlc) ->
              accumulatorMap.compute(
                  id,
                  (k, maybeSinkFlux) -> ofNullable(maybeSinkFlux)
                      .map(sinkFlux -> {
                        sinkFlux.sink().tryEmitNext(sampledOhlc);
                        return sinkFlux;
                      })
                      .orElseGet(() -> getSimpleSinkFlux(id, sampledOhlc))
              ));
          return accumulatorMap;
        })
        .log()
        .publish();

    /*
    Problem: the websocket shows the same item 5-10 times.
    Every 1 second, the fluxOfMap emits
    the websocket subscribes to all the topics in this 1-second Map,
    then when it emits again, the websocket subscribes again to the same subjects.
     */

    publishers.values().forEach(p -> p.publisher().connect());
    fluxOfMap.connect();
  }

  private static SinkFlux getSimpleSinkFlux(SourceIdOhlcId id, OHLC sampledOhlc) {
    var sink = Sinks
        .many()
        .replay()
        .<OHLC>latest();
    var flux = sink
        .asFlux()
        .scan(OHLC::mergeUpdates)
        .share()
        .replay(1);
    sink.tryEmitNext(sampledOhlc);
    flux.connect();
    return new SinkFlux(id, sink, flux);
  }

  private static Flux<ConcurrentMap<SourceIdOhlcId, OHLC>> getSampledMapForPublisher(ConnectableFlux<OHLC> publisher, int nbOfOhlcIds) {
    return publisher
        .scan(
            new ConcurrentHashMap<>((int) (nbOfOhlcIds * 1.5) + 1),
            Consumer::addEntryToMap)
        .sample(Duration.ofSeconds(1));
  }

  private static ConcurrentMap<SourceIdOhlcId, OHLC> addEntryToMap(ConcurrentMap<SourceIdOhlcId, OHLC> acc, OHLC next) {
    acc.compute(
        new SourceIdOhlcId(next.sourceId(), next.id()),
        (k, maybeV) -> ofNullable(maybeV)
            .map(v -> mergeUpdates(v, next))
            .orElse(next));
    return acc;
  }

}
