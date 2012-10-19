package com.umbrant.quantile;

public class Quantile {
  public final double quantile;
  public final double error;

  public Quantile(double quantile, double error) {
    this.quantile = quantile;
    this.error = error;
  }
}