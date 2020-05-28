KairosDb Extensions Release Notes
=================================

2.2.0 - June 5, 2020
------------------------
* Added version 2 Protobuf based storage from [kairosdb-format](https://github.com/InscopeMetrics/kairosdb-format).
* Histogram bucket counts represented as 64-bit values.

**NOTE**: To use the Protobuf based format you must update your configuration to:
```
kairosdb.service.histograms=io.inscopemetrics.kairosdb.HistogramModule
kairosdb.datapoints.factory.histogram=io.inscopemetrics.kairosdb.HistogramDataPointV2Factory
```

This is already set in the published [Docker image](https://hub.docker.com/r/inscopemetrics/kairosdb-extensions) as of version `2.2.0`.

2.1.10 - February 26, 2020
------------------------
* Upgrade `getSampleCount` method on `HistogramDataPointImpl` to use a `long`.

2.1.9 - February 19, 2020
------------------------
* Convert `Histogram` bucket count and sample count to `long`.
* Update `HistogramCountAggregator` to use `long`.
* Suppress infinite values in `HistogramDataPointFactory`.

***NOTE:*** Release 2.1.8 did not complete fully.

2.1.7 - July 9, 2019
------------------------
* Initial release to `io.inscopemetrics.kairosdb`

Published under Apache Software License 2.0, see LICENSE

&copy; Inscope Metrics, 2019
