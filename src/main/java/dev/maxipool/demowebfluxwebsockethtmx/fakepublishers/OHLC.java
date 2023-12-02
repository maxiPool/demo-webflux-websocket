package dev.maxipool.demowebfluxwebsockethtmx.fakepublishers;

import lombok.Builder;
import lombok.With;

import java.util.Random;

@Builder
@With
public record OHLC(int index, float open, float high, float low, float close) {

  static OHLC createOHLC(Random random) {
    float open = 100 + random.nextFloat() * 10; // Random open price
    float high = open + random.nextFloat() * 5;  // Random high price
    float low = open - random.nextFloat() * 5;   // Random low price
    float close = low + random.nextFloat() * 10; // Random close price

    return new OHLC(0, open, high, low, close);
  }

  static OHLC getNextOHLC(OHLC previous, Random random) {
    float open = previous.open() + (random.nextFloat() > .6f ? 1f : -1.1f) * random.nextFloat() * 2f; // Random open price
    float high = open + random.nextFloat();  // Random high price
    float low = open - random.nextFloat();   // Random low price
    float close = low + random.nextFloat() * 2f; // Random close price

    return new OHLC(previous.index() + 1, open, high, low, close);
  }

}
