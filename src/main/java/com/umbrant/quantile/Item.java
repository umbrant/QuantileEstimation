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

public class Item {
  public final long value;
  public int g;
  public final int delta;

  public Item(long value, int lower_delta, int delta) {
    this.value = value;
    this.g = lower_delta;
    this.delta = delta;
  }
  
  @Override
  public String toString() {
    return String.format("%d, %d, %d", value, g, delta);
  }
}
