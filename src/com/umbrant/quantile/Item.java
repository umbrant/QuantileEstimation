package com.umbrant.quantile;

public class Item {
  public final long value;
  public int g;
  public final int delta;

  public Item(long value, int lower_delta, int delta) {
    this.value = value;
    this.g = lower_delta;
    this.delta = delta;
  }
}