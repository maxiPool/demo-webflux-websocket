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
    disposables = allFlux
        .stream()
        .map(
            f -> f
                .subscribe(i -> {
                  if (i.id() == 0) {
                    log.info("index 0 {}", i);
                  }
                  messagingTemplate
                      .convertAndSend("/topic/" + i.id(), i);
                }))
        .toList();
  }

}
