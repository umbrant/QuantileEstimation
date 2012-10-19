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

public class Quantile {
  public final double quantile;
  public final double error;
  public final double u;
  public final double v;

  public Quantile(double quantile, double error) {
    this.quantile = quantile;
    this.error = error;
    u = 2.0 * error / (1.0 - quantile);
    v = 2.0 * error / quantile;
  }

  @Override
  public String toString() {
    return String.format("Q{q=%.3f, eps=%.3f})", quantile, error);
  }
}

