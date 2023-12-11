package dev.maxipool.demowebfluxwebsockethtmx.consumer;

import dev.maxipool.demowebfluxwebsockethtmx.fakepublishers.PublisherFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.tools.agent.ReactorDebugAgent;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

class ConsumerTest {

  private Consumer consumer;

  @BeforeEach
  void beforeEach() {
    ReactorDebugAgent.init();
    ReactorDebugAgent.processExistingClasses();

    final var factory = new PublisherFactory();
    consumer = new Consumer(factory);
  }

  @Test
  void should_consumeManyIdSingleSourceVersion1() throws InterruptedException {
    sleep(100);
    var fluxId0 = consumer.getByIdVersion1(0);

//    fluxId0.connect(); // connect tells the flux it can start emitting values; otherwise nothing happens.
    var veryFirst = fluxId0.blockFirst();
    System.out.println("veryFirst: " + veryFirst);
    sleep(500);
    var veryFirst2 = fluxId0.blockFirst();
    System.out.println("veryFirst2: " + veryFirst2);

    // > 1 second has passed, < 2 seconds
    sleep(500);
    var byIdVersion1A = fluxId0.blockFirst();
    System.out.println("byIdVersion1A: " + byIdVersion1A);

    // > 4 seconds have passed, < 5 seconds
    sleep(3000);
    var byIdVersion1B = fluxId0.blockFirst();
    System.out.println("byIdVersion1B: " + byIdVersion1B);

    assertThat(byIdVersion1A).isNotNull();
    assertThat(byIdVersion1B).isNotNull();
    assertThat(byIdVersion1B.index()).isGreaterThan(byIdVersion1A.index());
  }

}
