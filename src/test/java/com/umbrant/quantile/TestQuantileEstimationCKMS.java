package com.umbrant.quantile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.junit.Test;

public class TestQuantileEstimationCKMS {

  private static Logger LOG = Logger.getLogger(QuantileEstimationCKMS.class);
  static {
    BasicConfigurator.configure();
    LOG.setLevel(Level.INFO);
  }

  @Test
  public void TestCKMS() {
  
    final int window_size = 1000000;
    boolean generator = false;
  
    List<Quantile> quantiles = new ArrayList<Quantile>();
    quantiles.add(new Quantile(0.50, 0.050));
    quantiles.add(new Quantile(0.90, 0.010));
    quantiles.add(new Quantile(0.95, 0.005));
    quantiles.add(new Quantile(0.99, 0.001));
  
    QuantileEstimationCKMS estimator = new QuantileEstimationCKMS(
        quantiles.toArray(new Quantile[] {}));
  
    LOG.info("Inserting into estimator...");
  
    long startTime = System.currentTimeMillis();
    Random rand = new Random(0xDEADBEEF);
  
    if (generator) {
      for (int i = 0; i < window_size; i++) {
        estimator.insert(rand.nextLong());
      }
    } else {
      Long[] shuffle = new Long[window_size];
      for (int i = 0; i < shuffle.length; i++) {
        shuffle[i] = (long) i;
      }
      Collections.shuffle(Arrays.asList(shuffle), rand);
      for (long l : shuffle) {
        estimator.insert(l);
      }
    }
  
    for (Quantile quantile : quantiles) {
      double q = quantile.quantile;
      try {
        long estimate = estimator.query(q);
        long actual = (long) ((q) * (window_size - 1));
        double off = ((double) Math.abs(actual - estimate)) / (double) window_size;
        LOG.info(String.format("Q(%.2f, %.3f) was %d (off by %.3f)",
            quantile.quantile, quantile.error, estimate, off));
      } catch (IOException e) {
        LOG.info("No samples were present, could not query quantile.");
      }
    }
    LOG.info("# of samples: " + estimator.sample.size());
    LOG.info("Time (ms): " + (System.currentTimeMillis() - startTime));
  }
}
