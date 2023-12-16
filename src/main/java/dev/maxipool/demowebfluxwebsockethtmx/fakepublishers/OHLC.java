package dev.maxipool.demowebfluxwebsockethtmx.fakepublishers;

import lombok.Builder;
import lombok.With;

import java.util.Random;

@Builder
@With
public record OHLC(int sourceId, int id, int index, float open, float high, float low, float close) {

  static OHLC createOHLC(Random random, int sourceId) {
    float open = 100 + random.nextFloat() * 10; // Random open price
    float high = open + random.nextFloat() * 5;  // Random high price
    float low = open - random.nextFloat() * 5;   // Random low price
    float close = low + random.nextFloat() * 10; // Random close price

    return new OHLC(sourceId, 0, 0, open, high, low, close);
  }

  /**
   * Provide partial updates (to simulate a stream where only the modified fields are emitted).
   */
  static OHLC getNextOHLC(OHLC previous, Random random, int nbOfOhlcIds, int sourceId) {
    float open = previous.open() + (random.nextFloat() > .6f ? 1f : -1.1f) * random.nextFloat() * 2f; // Random open price
    float high = open + random.nextFloat();  // Random high price
    float low = open - random.nextFloat();   // Random low price
    float close = low + random.nextFloat() * 2f; // Random close price

    return new OHLC(
        sourceId,
        (previous.index() + 1) % nbOfOhlcIds,
        previous.index() + 1,
        random.nextFloat() > 0.5f ? open : 0f,
        random.nextFloat() > 0.5f ? high : 0f,
        random.nextFloat() > 0.5f ? low : 0f,
        random.nextFloat() > 0.5f ? close : 0f
    );
  }

  /**
   * Merge method to allow for updates sampling.
   * <br /><br />
   * Notes:
   * <ul>
   * <li>Within the .scan operator, one tactic to save cpu time might be to keep the latest emitted item for each field.</li>
   * <li>Also, if not every field was populated during the sampling, it means the emitted item will be missing some fields.</li>
   * <li>This implies we have to merge it with the value that was previously emitted from the sampler to avoid the stream to emit incomplete OHLC!</li>
   * </ul>
   */
  public static OHLC mergeUpdates(OHLC acc, OHLC next) {
    var open = next.open() != 0f ? next.open() : acc.open();
    var high = next.high() != 0f ? next.high() : acc.high();
    var low = next.low() != 0f ? next.low() : acc.low();
    var close = next.close() != 0f ? next.close() : acc.close();

    return new OHLC(acc.sourceId(), next.id(), next.index(), open, high, low, close);
  }

}
