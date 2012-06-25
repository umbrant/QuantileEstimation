QuantileEstimation
==================

Implementation of algorithms for streaming estimation of percentiles, especially high-percentiles (e.g. 90th, 95th, 99th).
This is hugely useful for keeping latency metrics for any type of service, such as your storage system or network.
Efficiency in both space and time are major concerns; your metrics shouldn't impact the performance of the system!

Requirements
============
* Multiplicative, not additive, error bounds on quantile estimates. For example, I want 1% error on my 90th percentile estimate, and 0.1% error on my 99th percentile estimate.
* View of the data over multiple sliding time windows, e.g. last 10s, 1m, 5m, 15m.
* Efficiency: need sub-linear bounds on space and time complexity.

Reading
=======

There's a lot of prior work in this area within the database and theory community.
I'm just trying to implement these and get a feel for what best suits my needs.

* Cormode, Korn, Muthukrishnan, and Srivastava. "Effective Computation of Biased Quantiles over Data Streams" in ICDE 2005.
* Greenwald and Khanna. "Space-efficient online computation of quantile summaries" in SIGMOD 2001.