package dev.maxipool.demowebfluxwebsockethtmx.websocket;

import dev.maxipool.demowebfluxwebsockethtmx.consumer.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.broker.BrokerAvailabilityEvent;
import org.springframework.stereotype.Service;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;
import static reactor.core.publisher.Flux.fromIterable;

@Slf4j
@RequiredArgsConstructor
@Service
public class WsBroadcaster {

  private static final String TOPIC_TEMPLATE = "/topic/%d/%d";
  private static final Scheduler WS_PUBLISHER_THREAD = Schedulers.newSingle("web-socket-publisher-thread");
  private final AtomicBoolean brokerAvailable = new AtomicBoolean(false);

  private final SimpMessagingTemplate messagingTemplate;
  private final Consumer consumer;

  @EventListener
  public void onApplicationEvent(BrokerAvailabilityEvent event) {
    brokerAvailable.set(event.isBrokerAvailable());
//    broadcastQuotes();
    broadcastQuotesUsingTheMergedFluxesAkaV2();
//    broadcastQuotesUsingV3();
  }

  // TODO: fix bug
  //       If there are new subjects in the Map of SinkFlux, they won't be added to the
  //       websocket broadcast. I need to come up with a solution to add/remove.
  //       Here's one solution: {@code Flux<Map<id,Flux<OHLC>>> }
  public void broadcastQuotesUsingV3() {
    broadcastHelper(() -> {
      // now I want to subscribe to the Flux<OHLC> and when there's a new one, unsub,resub
      // It's too complicated to use a Map to add new topics and remove deactivated topics.
      // instead I should listen to a List of Flux to be added and simply add a subscription.


    });
  }

  /**
   * Just put all the flux into a single one and subscribe to it to publish on the websocket.
   * <br />
   * This means a single thread will be pushing on the websocket queue now.
   */
  public void broadcastQuotesUsingTheMergedFluxesAkaV2() {
    broadcastHelper(() -> {
      var allFlux = consumer.getAllFlux();
      log.warn("Flux/topics to subscribe to: {}", allFlux.size());
      fromIterable(allFlux)
          .flatMap(f -> f)
          .publishOn(WS_PUBLISHER_THREAD)
          .subscribe(
              i -> messagingTemplate.convertAndSend(TOPIC_TEMPLATE.formatted(i.sourceId(), i.id()), i));
    });
  }

  /**
   * This way of doing thing implies many threads (at most 1 per subscribed Flux), can push to the websocket
   * queue concurrently.
   */
  public void broadcastQuotes() {
    broadcastHelper(() -> {
      var allFlux = consumer.getAllFluxWithMetadata();
      log.warn("Flux/topics to subscribe to: {}", allFlux.size());
      final var source0ohlc0 = new Consumer.SourceIdOhlcId(0, 0);
      allFlux.forEach(
          f -> f
              .flux()
              .publishOn(WS_PUBLISHER_THREAD)
              .subscribe(i -> {
                if (f.id().ohlcId() == source0ohlc0.ohlcId()) {
                  log.info("source {} ohlc 0 {}", f.id().sourceId(), i);
                }
                messagingTemplate
                    .convertAndSend(TOPIC_TEMPLATE.formatted(f.id().sourceId(), f.id().ohlcId()), i);
              }));
    });
  }

  private void broadcastHelper(Runnable runnable) {
    if (!brokerAvailable.get()) {
      log.warn("Websocket Broker not available yet; not going to broadcast quotes; Shutting down my app!");
//      System.exit(-1);
      return;
    }
    mySleep();

    runnable.run();
  }

  @SneakyThrows
  private static void mySleep() {
    sleep(1000);
  }

}
