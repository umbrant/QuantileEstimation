/*
   Copyright 2012 Andrew Wang (andrew@umbrant.com)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.umbrant.quantile;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Implementation of the Greenwald and Khanna algorithm for streaming
 * calculation of epsilon-approximate quantiles.
 * 
 * See: Greenwald and Khanna,
 * "Space-efficient online computation of quantile summaries" in SIGMOD 2001
 * 
 */
public class QuantileEstimationGK {

  private static Logger LOG = Logger.getLogger(QuantileEstimationGK.class);
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
  
  public QuantileEstimationGK(double epsilon, int compact_size) {
    this.compact_size = compact_size;
    this.epsilon = epsilon;
  }

  List<Item> sample = Collections.synchronizedList(new LinkedList<Item>());

  private void printList() {
    StringBuffer buf = new StringBuffer();
    for (Item i : sample) {
      buf.append(String.format("(%d %d %d),", i.value, i.g, i.delta));
    }
    LOG.debug(buf.toString());
  }

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
      delta = (int) Math.floor(2 * epsilon * count);
    }
    
    Item newItem = new Item(v, 1, delta);
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
    QuantileEstimationGK estimator = new QuantileEstimationGK(epsilon, 1000);
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
