package dev.maxipool.demowebfluxwebsockethtmx.websocket;

import dev.maxipool.demowebfluxwebsockethtmx.consumer.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.broker.BrokerAvailabilityEvent;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

@Slf4j
@RequiredArgsConstructor
@Service
public class Broadcaster {

  private static final String TOPIC_TEMPLATE = "/topic/%d/%d";

  private final SimpMessagingTemplate messagingTemplate;
  private final AtomicBoolean brokerAvailable = new AtomicBoolean(false);

  private final Consumer consumer;

  private List<Disposable> disposables;

  @EventListener
  public void onApplicationEvent(BrokerAvailabilityEvent event) {
    brokerAvailable.set(event.isBrokerAvailable());
    broadcastQuotes();
  }

  public void broadcastQuotes() {
    if (!brokerAvailable.get()) {
      log.warn("Websocket Broker not available yet; not broadcasting quotes");
      return;
    }
    try {
      sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // TODO: fix bug
    //       If there are new subjects in the Map of SinkFlux, they won't be added to the
    //       websocket broadcast. I need to come up with a solution to add/remove.
    //       Here's one solution: {@code Flux<Map<id,Flux<OHLC>>> }
    var allFlux = consumer.getAllFlux();
    log.warn("Flux/topics to subscribe to: {}", allFlux.size());
    final var source0ohlc0 = new Consumer.SourceIdOhlcId(0, 0);
    disposables = allFlux
        .stream()
        .map(
            f -> f
                .flux()
                .subscribe(i -> {
                  if (f.id().ohlcId() == source0ohlc0.ohlcId()) {
                    log.info("source {} ohlc 0 {}", f.id().sourceId(), i);
                  }
                  messagingTemplate
                      .convertAndSend(TOPIC_TEMPLATE.formatted(f.id().sourceId(), f.id().ohlcId()), i);
                }))
        .toList();
  }

}
