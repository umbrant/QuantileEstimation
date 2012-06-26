package com.umbrant.quantile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Implementation of the Cormode, Korn, Muthukrishnan, and Srivastava algorithm
 * for streaming calculation of targeted high-percentile epsilon-approximate
 * quantiles.
 * 
 * This is a generalization of the earlier work by Greenwald and Khanna (GK),
 * which essentially allows different error bounds on the targeted quantiles,
 * which allows for far more efficient calculation of high-percentiles.
 * 
 * 
 * See: Cormode, Korn, Muthukrishnan, and Srivastava
 * "Effective Computation of Biased Quantiles over Data Streams" in ICDE 2005
 * 
 * Greenwald and Khanna,
 * "Space-efficient online computation of quantile summaries" in SIGMOD 2001
 * 
 */
public class QuantileEstimationCKMS {

  private static Logger LOG = Logger.getLogger(QuantileEstimationCKMS.class);
  static {
    BasicConfigurator.configure();
    LOG.setLevel(Level.INFO);
  }

  // Total number of items in stream
  int count = 0;
  // Threshold to trigger a compaction
  final int compact_size;

  List<Item> sample = Collections.synchronizedList(new LinkedList<Item>());
  final Quantile quantiles[];

  public QuantileEstimationCKMS(int compact_size, Quantile[] quantiles) {
    this.compact_size = compact_size;
    this.quantiles = quantiles;
  }

  /**
   * Specifies the allowable error for this rank, depending on which
   * quantiles are being targeted.
   * 
   * This is the f(r_i, n) function from the CKMS paper. It's basically how wide
   * the range of this rank can be.
   * 
   * @param rank
   */
  private double allowableError(int rank) {
    int size = sample.size();
    double minError = size + 1;
    for (Quantile q : quantiles) {
      double error;
      if (rank <= q.quantile * size) {
        error = (2.0 * q.error * (size - rank)) / (1.0 - q.quantile);
      } else {
        error = (2.0 * q.error * rank) / q.quantile;
      }
      if (error < minError) {
        minError = error;
      }
    }

    return minError;
  }

  private void printList() {
    StringBuffer buf = new StringBuffer();
    for (Item i : sample) {
      buf.append(String.format("(%d %d %d),", i.value, i.g, i.delta));
    }
    LOG.debug(buf.toString());
  }

  /**
   * Add a new value from the stream.
   * 
   * @param v
   */
  public void insert(long v) {
    int idx = 0;
    for (Item i : sample) {
      if (i.value > v) {
        break;
      }
      idx++;
    }

    int delta;
    if (idx == 0 || idx == sample.size()) {
      delta = 0;
    } else {
      delta = ((int) Math.floor(allowableError(idx))) - 1;
    }

    Item newItem = new Item(v, 1, delta);
    sample.add(idx, newItem);

    if (sample.size() % compact_size == 0) {
      printList();
      compress();
      printList();
    }
    this.count++;
  }

  /**
   * Try to remove extraneous items from the set of sampled items. This checks
   * if an item is unnecessary based on the desired error bounds, and merges it
   * with the adjacent item if it is.
   */
  public void compress() {
    int removed = 0;

    for (int i = 0; i < sample.size() - 1; i++) {
      Item item = sample.get(i);
      Item item1 = sample.get(i + 1);

      // Merge the items together if we don't need it to maintain the
      // error bound
      if (item.g + item1.g + item1.delta <= allowableError(i)) {
        item1.g += item.g;
        sample.remove(i);
        removed++;
      }
    }
    LOG.debug("Removed " + removed + " items");
  }

  /**
   * Get the estimated value at the specified quantile.
   * 
   * @param quantile Queried quantile, e.g. 0.50 or 0.99.
   * @return Estimated value at that quantile.
   */
  public long query(double quantile) {
    int rankMin = 0;
    int desired = (int) (quantile * count);

    for (int i = 1; i < sample.size(); i++) {
      Item prev = sample.get(i - 1);
      Item cur = sample.get(i);

      rankMin += prev.g;

      if (rankMin + cur.g + cur.delta > desired + allowableError(i)) {
        return prev.value;
      }
    }

    // edge case of wanting max value
    return sample.get(sample.size() - 1).value;
  }

  public static void main(String[] args) {

    final int window_size = 50000;
    List<Quantile> quantiles = new ArrayList<Quantile>();
    quantiles.add(new Quantile(0.50, 0.050));
    quantiles.add(new Quantile(0.90, 0.010));
    quantiles.add(new Quantile(0.95, 0.005));
    quantiles.add(new Quantile(0.99, 0.001));

    LOG.info("Generating random longs...");
    long[] shuffle = new long[window_size];
    for (int i = 0; i < shuffle.length; i++) {
      shuffle[i] = i;
    }
    Random rand = new Random(0xDEADBEEF);
    Collections.shuffle(Arrays.asList(shuffle), rand);

    LOG.info("Inserting into estimator...");
    QuantileEstimationCKMS estimator = new QuantileEstimationCKMS(100,
        quantiles.toArray(new Quantile[] {}));
    for (long l : shuffle) {
      estimator.insert(l);
    }

    for (Quantile quantile : quantiles) {
      double q = quantile.quantile;
      long estimate = estimator.query(q);
      long actual = (long) ((q) * (window_size - 1));
      LOG.info(String.format("Q(%.2f, %.3f) was %d (off by %.3f)",
          quantile.quantile, quantile.error, estimate,
          ((double) Math.abs(actual - estimate)) / (double) window_size));
    }
    LOG.info("# of samples: " + estimator.sample.size());
  }
}
