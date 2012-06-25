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
 * See:
 * Cormode, Korn, Muthukrishnan, and Srivastava
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

  // Acceptable % error in percentile estimate
  final double epsilon;
  // Total number of items in stream
  int count = 0;
  // Threshold to trigger a compaction
  final int compact_size;
  
  public QuantileEstimationCKMS(double epsilon, int compact_size) {
    this.compact_size = compact_size;
    this.epsilon = epsilon;
  }

  List<Item> sample = Collections.synchronizedList(new LinkedList<Item>());

  class Item {

    protected long value;
    protected int g;
    protected int delta;

    public Item(long value, int lower_delta, int delta) {
      this.value = value;
      this.g = lower_delta;
      this.delta = delta;
    }
  }

  private void printList() {
    StringBuffer buf = new StringBuffer();
    for (Item i : sample) {
      buf.append(String.format("(%d %d %d),", i.value, i.g, i.delta));
    }
    LOG.debug(buf.toString());
  }

  public void insert(long v) {
    Item newItem = new Item(v, 1, 0);
    int idx = 0;

    for (Item i : sample) {
      if (i.value > v) {
        break;
      }
      idx++;
    }

    if (idx == 0 || idx == sample.size()) {
      newItem.delta = 0;
    } else {
      newItem.delta = (int) Math.floor(2 * epsilon * count);
    }

    sample.add(idx, newItem);
    if(sample.size() > compact_size) {
      printList();
      compress();
      printList();
    }
    this.count++;
  }

  public void compress() {
    int removed = 0;

    for (int i = 0; i < sample.size() - 1; i++) {
      Item item = sample.get(i);
      Item item1 = sample.get(i + 1);

      // Merge the items together if we don't need it to maintain the
      // error bound
      if (item.g + item1.g + item1.delta <= Math.floor(2 * epsilon * count)) {
        item1.g += item.g;
        sample.remove(i);
        removed++;
      }
    }
    LOG.debug("Removed " + removed + " items");
  }

  public long query(double quantile) {
    int rankMin = 0;
    int desired = (int) (quantile * count);

    for (int i = 1; i < sample.size(); i++) {
      Item prev = sample.get(i - 1);
      Item cur = sample.get(i);

      rankMin += prev.g;

      if (rankMin + cur.g + cur.delta > desired + (2 * epsilon * count)) {
        return prev.value;
      }
    }

    // edge case of wanting max value
    return sample.get(sample.size() - 1).value;
  }

  public static void main(String[] args) {

    final int window_size = 10000;
    final double epsilon = 0.001;

    LOG.info("Generating random longs...");
    long[] shuffle = new long[window_size];
    for (int i = 0; i < shuffle.length; i++) {
      shuffle[i] = i;
    }
    Random rand = new Random(0xDEADBEEF);
    Collections.shuffle(Arrays.asList(shuffle), rand);

    LOG.info("Inserting into estimator...");
    QuantileEstimationCKMS estimator = new QuantileEstimationCKMS(epsilon, 1000);
    for (long l : shuffle) {
      estimator.insert(l);
    }

    double[] quantiles = { 0.5, 0.9, 0.95, 0.99, 1.0 };
    for (double q : quantiles) {
      long estimate = estimator.query(q);
      long actual = (long) ((q) * (window_size - 1));
      LOG.info(String.format("Estimated %.2f quantile as %d (actually %d)", q,
          estimate, actual));
    }
    LOG.info("# of samples: " + estimator.sample.size());
  }
}
