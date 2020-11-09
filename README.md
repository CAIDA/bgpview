
# BGPView

BGPView is a set of highly optimized data structures and libraries for
(re-)construction, transport and analysis of BGP "routing tables".

# Table of Contents
- [BGPView](#bgpview)
  * [Background](#background)
  * [Quick Start](#quick-start)
    + [Debian/Ubuntu Packages](#debian-ubuntu-packages)
    + [Building From Source](#building-from-source)
  * [Realtime Analysis](#realtime-analysis)
    + [CAIDA's Public BGPView feed](#caida-s-public-bgpview-feed)
      - [Kafka Cluster URL](#kafka-cluster-url)
      - [View Publication Timeouts](#view-publication-timeouts)
      - [Example Usage](#example-usage)
    + [Running a private BGPView deployment](#running-a-private-bgpview-deployment)
  * [Offline Analysis](#offline-analysis)
    + [BSRT Options](#bsrt-options)
      - [Common Options](#common-options)
    + [One-off Processing](#one-off-processing)
    + [Spark-managed Processing](#spark-managed-processing)
  * [Available Consumers](#available-consumers)
    + [Utility Consumers](#utility-consumers)
      - [`archiver`](#archiver)
      - [`view-sender`](#view-sender)
      - [`visibility`](#visibility)
    + [Prefix-Origin Consumers](#prefix-origin-consumers)
      - [`peer-pfx-origins`](#peer-pfx-origins)
      - [`pfx2as`](#pfx2as)
    + [BGP Hijacks Observatory Consumers](#bgp-hijacks-observatory-consumers)
      - [`edges`](#edges)
      - [`moas`](#moas)
      - [`subpfx`](#subpfx)
      - [`announced-pfxs`](#announced-pfxs)
      - [`pfx-origins`](#pfx-origins)
      - [`routed-space`](#routed-space)
      - [`triplets`](#triplets)
    + [IODA Consumers](#ioda-consumers)
      - [`per-as-visibility`](#per-as-visibility)
      - [`per-geo-visibility`](#per-geo-visibility)
    + [Test/Template Consumers](#test-template-consumers)
      - [`test`](#test)
      - [`myviewprocess`](#myviewprocess)
    + [Misc Consumers](#misc-consumers)
  * [Writing a New Consumer](#writing-a-new-consumer)
  

## Background

For a detailed description of BGPView, see Section 6.2 of the
[BGPStream paper](https://www.caida.org/publications/papers/2016/bgpstream/bgpstream.pdf).

At a high level, the goal of BGPView is to facilitate the inference of
"Global" routing tables at a finer granularity than the RIB dumps
provided by the RouteViews and RIPE RIS projects. For example,
currently RouteViews collectors export a snapshot of their RIB (a "RIB
dump") every 2 hours -- for RIPE RIS this is once every 8 hours. For
applications where latency matters (e.g., near-realtime event
detection), and/or are interested in observing short-duration events,
they cannot rely on these RIB dumps alone (i.e., using RouteViews
data, they would only be able to observe events that last at least 2
hours, and with up to 2 hours of latency).

The normal approach to solving this problem is to write some code that
starts with a RIB dump, and then incrementally applies update
information to approximate the state of each peer's routing table at
any point in time. Then, depending on the application, one can either
react to specific events (e.g., a prefix is announced, withdrawn,
etc.) or, periodically walk through these routing tables and perform
analysis on the entire "global" routing table (or, "inferred"
RIB). BGPView is designed to make it simple to write analysis code in
the latter model, with all of the details of obtaining the raw BGP
data, processing it, and inferring the routing table for each peer are
abstracted away from the user. The user is instead able to focus on
writing an analysis kernel (a "BGPView Consumer") that is invoked
every time a new inferred RIB (a "BGPView") is available.

There are two primary modes of operation for BGPView: realtime, and
offline.

In realtime mode, there is a set of distributed processes that:
 - obtain the BGP data (using
   [BGPStream](https://bgpstream.caida.org)) as soon as it is made
   available by the upstream projects
 - process it, and
 - periodically (every 5 min in CAIDA's reference deployment) publish
   fragments of each view to a Kafka cluster.
Users can then connect their analysis modules to this cluster using
the BGPView toolset, and as soon as a view becomes available, their
code will analyze it.

In offline mode the BGPView components are similar, but rather than a
distributed set of many processes that communicate via Kafka,
everything runs within a single process. The views that are generated
are passed in-memory to the configured consumers. While this is a
convenient method for using the same analysis code (consumers) to do
longitudinal analysis, it does require significant memory (often >20
GB when using all collectors), and is not a parallelized application,
so processing times are roughly the sum of all components that operate
in the realtime pipeline (whereas, in realtime these components are
truly pipelined).

## Quick Start

### Debian/Ubuntu Packages

The easiest way to install BGPView and its dependencies is from
CAIDA's apt package mirror.

```
curl https://pkg.caida.org/os/ubuntu/bootstrap.sh | bash
sudo apt install bgpview
```
Of course you should first manually inspect the `bootstrap.sh` script
before executing it.

### Building From Source

You will need to first install dependencies:

 - [libbgpstream (>= v2.0.0)](https://bgpstream.caida.org)
 - [libtimeseries (>= v1.0.0)](https://github.com/CAIDA/libtimeseries)
 - [libipmeta (>= v3.0.0)](https://github.com/CAIDA/libipmeta)
 - [libwandio (>= v4.2.0)](https://research.wand.net.nz/software/libwandio.php)
 - [librdkafka (>= v0.11.3)](https://github.com/edenhill/librdkafka)

On Ubuntu/Debian systems, this means something like
```
sudo apt install libbgpstream2-dev libtimeseries0-dev \
  libipmeta2-dev libwandio1-dev librdkafka-dev
```
This assumes you have added the CAIDA package archive to apt using the
bootstrap command above.

One you have installed the dependencies, download the BGPView source,
either from the
[GitHub releases page](https://github.com/CAIDA/bgpview/releases), or
by cloning the GitHub repo.

If you cloned from GitHub, you will first need to:
```
./autogen.sh
```

Then:
```
./configure
make
sudo make install
```

This will install the `bgpview-consumer` tool which is used to run the
consumers against a source of BGPViews.

There is also a `bvcat` tool that will be installed. This is used to
convert the binary files generated by the `archiver` consumer into
ASCII format.

**One-step installation script** for both bgpview and its dependencies is [available here](https://github.com/CAIDA/bgpview/wiki/One-step-installation-from-sources).

## Realtime Analysis

### CAIDA's Public BGPView feed

Because running the realtime BGPView system requires significant
compute infrastructure, CAIDA is making its BGPView feed publicly
accessible. Users can install the BGPView software on their local
servers, and then configure the `kafka` interface to consume data from
the CAIDA Kafka cluster.

#### Kafka Cluster URL

The production Kafka cluster is available at:
```
bgpview.bgpstream.caida.org:9192
```
_Note the non-standard 9192 port number._

#### View Publication Timeouts

Reasoning about timeouts and trade-offs are in the [BGPStream paper](https://www.caida.org/publications/papers/2016/bgpstream/bgpstream.pdf), specifically check Section 6.2.3 (Data synchronization). In the Observatory and IODA we use a 40 min time-out based on historical observation of when MRT files are made available by the RouteViews and RIPE RIS web archives.

Available timeouts:
 - 10 min (`-c 600`)
 - 20 min (`-c 1200`)
 - 30 min (`-c 1800`)
 - 40 min (`-c 2400`)
 - 50 min (`-c 3000`)
 - 1 hr (`-c 3600`)
 - 2 hr (`-c 7200`)
 - 4 hr (`-c 14400`)
 - 6 hr (`-c 21600`)

#### Example Usage

```
bgpview-consumer \
  -i "kafka -k bgpview.bgpstream.caida.org:9192 -n bgpview-prod -c 2400" \
  -N 1 \
  -b ascii \
  -c "test"
```

In this example, we configure the `kafka` IO module to:

 - connect to the Kafka cluster running at
   `bgpview.bgpstream.caida.org:9192` (note the non-standard 9192 port
   number).
 - use the `bgpview-prod` "namespace" in this cluster (this is the
   only available namespace so should not be changed)
 - use the 2400-second timeout view stream (see notes above about
   timeouts)
 - process a single view before exiting (`-N `)
 - use the `ascii` backend for writing timeseries statistics
 - run the `test` consumer (see below for how to run other consumers)

This will require a _significant_ amount of memory to run (> 8 GB).

**Note** that there are quotes around the arguments to the `-i`
option. This is because the `kafka -k ....` sub-command is passed
directly to the kafka module for argument processing. The same is true
when configuring consumers (here we don't pass arguments to the `test`
consumer, so the quotes are not strictly necessary).

To filter the view to a prefix (or origin ASN) of interest, the `-f`
option can be used as follows:
```
bgpview-consumer \
  -i "kafka -k bgpview.bgpstream.caida.org:9192 -n bgpview-prod -c 2400" \
  -N 1 \
  -b ascii \
  -c "test" \
  -f "pfx:192.172.226.0/24"
```
This uses "only" 1.5 GB of memory, and will output (amongst other
things) all of the AS paths observed toward the given prefix.

The BGPView code currently outputs a large amount of debugging
information
([Issue #25](https://github.com/CAIDA/bgpview/issues/25)). You may
want to filter it out by doing something like:
```
bgpview-consuer [arguments] 2>&1 | fgrep -v DEBUG | fgrep -v INFO | fgrep -v "AS Path Store"
```

### Running a private BGPView deployment

TODO (pjb)

## Offline Analysis

Offline analysis can be carried out using the same `bgpview-consumer`
tool that is used for realtime processing (see above).

The important difference between running BGPView in "offline mode"
compared to realtime, is that we switch to using the BGPStream
RealTime (`bsrt`) IO module rather than `kafka`. This is the same
module that is used in the View generation stage of the realtime
system, but in this case we don't configure it to transmit the
resulting Views to Kafka -- we simply pass them in-memory to our
consumers.

### BSRT Options

The options for the `bsrt` module are (deliberately) very similar to
those for the `bgpreader` tool:
```
$ bgpview-consumer -c test -b ascii -i "bsrt -h"
[14:07:41:815] timeseries_init: initializing libtimeseries
[14:07:41:815] timeseries_enable_backend: enabling backend (ascii)
INFO: Enabling consumer 'test'
INFO: Starting BSRT IO consumer module...
BSRT IO Options:
   -d <interface> use the given bgpstream data interface to find available data
                  available data interfaces are:
       broker       Retrieve metadata information from the BGPStream Broker service
       singlefile   Read a single mrt data file (RIB and/or updates)
       kafka        Read updates in real-time from an Apache Kafka topic
       csvfile      Retrieve metadata information from a csv file
   -o <option-name=option-value>*
                  set an option for the current data interface.
                  use '-o ?' to get a list of available options for the current
                  data interface. (data interface can be selected using -d)
   -p <project>   process records from only the given project (routeviews, ris)*
   -c <collector> process records from only the given collector*
   -t <type>      process records with only the given type (ribs, updates)*
   -w <start>[,<end>]
                  process records within the given time window
   -P <period>    process a rib files every <period> seconds (bgp time)
   -j <peer ASN>  return valid elems originated by a specific peer ASN*
   -k <prefix>    return valid elems associated with a specific prefix*
   -y <community> return valid elems with the specified community*
                  (format: asn:value, the '*' metacharacter is recognized)

   -i <interval>  distribution interval in seconds (default: 60)
   -a             align the end time of the first interval
   -g <gap-limit> maximum allowed gap between packets (0 is no limit) (default: 0)
   -n <name>      monitor name (default: gibi.caida.org)
   -O <outfile>   use <outfile> as a template for file names.
                   - %X => plugin name
                   - %N => monitor name
                   - see man strftime(3) for more options
   -r <intervals> rotate output files after n intervals
   -R <intervals> rotate bgpcorsaro meta files after n intervals

   -h             print this help menu
* denotes an option that can be given multiple times
```

#### Common Options

For most use, the `-i`, `-w`, `-c`, and/or `-p` options are all that
are necessary.

`-i` specifies the frequency with which views will be created and
provided to consumers. The public BGPView stream described above uses
`-i 300` to generate views for every 5 mins of BGP data processed.

`-w` specifies the time range (using seconds since the unix epoch) to
process. The syntax is `-w <start>[,<end>]`, but if the `end`
parameter is not provided, BSRT will go into "realtime" mode and
process data continuously, forever.

`-c` specifies the collectors to process data from (e.g.,
`route-views.sg`). This option can be given multiple times to include
data from multiple collectors.

`-p` specifies which BGP collection projects (e.g., `ris`) to process
data from. See https://bgpstream.caida.org/data for information about
supported projects, but at the time of writing the two projects that
are compatible with BGPView are `routeviews` and `ris` (since these
are the only two that provide RIB dumps which BGPView needs to
bootstrap processing). This option can be given multiple times to include
data from multiple projects.

*Warning:* While you are not required to specify either `-c` or `-p`,
if you do this, BSRT will process data from all available projects --
including those that do not provide RIB dumps (e.g.,
`routeviews-stream`). This may have unintended consequences. As a
result, it is recommended that if you do not need to limit processing
to a single collector, you should specify `-p routeviews -p ris`.

Note also that the `-O` option is required, but is a legacy from a
previous version of the library. We no longer make use of any of the
output files that are written to this location. You can safely set
this to something like `/tmp/%X.bgpview.deleteme` and ignore the
contents.

### One-off Processing

Example script for triggering a one-shot offline BGPView run (with the
archiver consumer):

```bash
#!/bin/bash

set -e

if [ "$#" -ne 3 ]
then
  echo "Usage: $0 collector start end"
  exit 1
fi

COLLECTOR="$1"
START="$2"
END="$3"

OUTDIR="/scratch/aspathsdb/bgpview/archiver"
mkdir -p $OUTDIR/logs
mkdir -p $OUTDIR/views

bgpview-consumer \
  -i "bsrt -i 300 -c $COLLECTOR -w $START,$END -O $OUTDIR/logs/%X.deleteme" \
  -b ascii \
  -c "archiver -f $OUTDIR/views/$COLLECTOR.%s.bgpview.gz -r 300 -m binary"
```

### Spark-managed Processing

TODO (mingwei)

## Available Consumers

All consumer code is located in the
[lib/consumers/](https://github.com/CAIDA/bgpview/tree/master/lib/consumers)
directory. Consumer source files are named `bvc_<consumer>.[ch]`.

To get a list of consumers built into a bgpview-consumer binary, run
it without arguments:
```
$ bgpview-consumer
[14:24:21:829] timeseries_init: initializing libtimeseries
...
       -c"<consumer> <opts>" Consumer to activate (can be used multiple times)
                               available consumers:
                                - test
                                - perfmonitor
                                - visibility
                                - per-as-visibility
                                - per-geo-visibility
                                - announced-pfxs
                                - moas
                                - archiver
                                - edges
                                - triplets
                                - pfx-origins
                                - routed-space
                                - my-view-process
                                - view-sender
                                - path-change
                                - subpfx
                                - peer-pfx-origins
                                - pfx2as
...
```

To see the usage for a specific consumer, run it like so:
```
bgpview-consumer -b ascii -i test -c "<CONSUMER> -h"
```
e.g.,
```
$ bgpview-consumer -b ascii -i test -c "archiver -h"
[09:52:52:870] timeseries_init: initializing libtimeseries
[09:52:52:870] timeseries_enable_backend: enabling backend (ascii)
INFO: Enabling consumer 'archiver'
consumer usage: archiver
       -f <filename> output file pattern for writing views
                       accepts same format parameters as strftime(3)
                       as well as '%s' to write unix time
       -r <seconds>  output file rotation period (default: no rotation)
       -a            disable alignment of output file rotation to multiples of the rotation interval
       -l <filename> file to write the filename of the latest complete output file to
       -c <level>    output compression level to use (default: 6)
       -m <mode>     output mode: 'ascii' or 'binary' (default: binary)
...
```

**Note:** the `-c` option can (and sometimes must) be used multiple
times to run multiple consumers on each View. In this configuration
the consumers will be "chained" in the order they are specified on the
command line, and each consumer will process each View in series.

Currently the following consumers are part of the official BGPView package:

### Utility Consumers

#### `archiver`

Serializes views to files (either in ASCII or compact binary format).

Usage:
```
consumer usage: archiver
       -f <filename> output file pattern for writing views
                       accepts same format parameters as strftime(3)
                       as well as '%s' to write unix time
       -r <seconds>  output file rotation period (default: no rotation)
       -a            disable alignment of output file rotation to multiples of the rotation interval
       -l <filename> file to write the filename of the latest complete output file to
       -c <level>    output compression level to use (default: 6)
       -m <mode>     output mode: 'ascii' or 'binary' (default: binary)
```

#### `view-sender`

Used in the realtime distributed system to publish views to Kafka.

Usage:
```
consumer usage: view-sender [options] -n <instance-name> -i <io-module>
       -i <module opts>      IO module to use for sending views.
                               Available modules:
                                - kafka
       -n <instance-name>    Unique name for this sender (required)
       -s <sync-interval>   Sync frame freq. in secs (default: 3600)
                               (used only for Kafka)
       -4 <pfx-cnt>          Only send peers with > N IPv4 pfxs (default: 400000)
       -6 <pfx-cnt>          Only send peers with > N IPv6 pfxs (default: 10000)
```

#### `visibility`

Calculates simple visibility information. Used by many other
consumers.

Usage:
```
consumer usage: visibility
       -4 <pfx-cnt>  # pfxs in a IPv4 full-feed table (default: 400000)
       -6 <pfx-cnt>  # pfxs in a IPv6 full-feed table (default: 10000)
       -m <mask-len> minimum mask length for pfxs (default: 6)
       -p <peer-cnt> # peers that must observe a pfx (default: 10)
```

### Prefix-Origin Consumers

#### `peer-pfx-origins`

"Cecilia's consumer". Generates "un-opinionated" per-peer
prefix-origin information.

Usage:
```
consumer usage: peer-pfx-origins
       -o <path>             output directory
       -c                    only output peer counts
```

Sample output:
```
# peer_cnt: 1362
prefix|origin|peer_cnt
58.30.222.0/24|17429|377
200.50.174.0/24|264797|371
82.117.160.0/20|31036|429
70.169.92.0/22|22773|419
```

#### `pfx2as`

Accumulates per-prefix origin information across many views (e.g., 
defaults to 24hrs) and writes summary information periodically. 
Used as input for the new high-level CAIDA Prefix2AS dataset.

Usage:
```
consumer usage: pfx2as
       -i <interval>  output interval in seconds (default 86400)
       -o <path>      output directory
       -f <fmt>       output format: "dsv" (default) or "json"
       -c             output peer counts, not full list
       -v             split prefixes into files by IP version
```

Sample output:
```
# D|<start>|<duration>|<monitor_cnt>|<pfx_cnt>
# M|<monitor_idx>|<collector>|<address>|<asn>|<pfx_cnt>
# P|<pfx>|<asn>|<full_cnt>|<partial_cnt>|<full_duration>|<partial_duration>
# p|<monitor_idx>|<duration>
D|1599652800|600|15|841574
M|1|route-views.eqix|206.126.237.22|808838|199524
M|2|route-views.eqix|206.126.236.172|816403|11039
M|3|route-views.eqix|206.126.238.56|818199|37468
...
...
...
P|23.230.96.0/24|18779|15|0|600|0
p|1|600
p|2|600
...
...
```
### BGP Hijacks Observatory Consumers

#### `edges`

Monitors AS paths and detects new "edges" (AS links).

#### `moas`

Identifies multiple-origin prefixes.

#### `subpfx`

Identifies sub-prefix announcements.

#### `announced-pfxs`

Aggregates data across views and outputs information about announced
prefixes.

Usage:
```
consumer usage: announced-pfxs
       -w <window-size>      window size in seconds (default 604800)
       -i <output-interval>  output interval in seconds (default 86400)
       -o <path>             output folder (default: current folder)
```

#### `pfx-origins`

Tracks changes in prefix origin ASes between views.

Usage:
```
consumer usage: pfx-origins
       -o <path>             output folder (default: ./)
```

Example output:
```
1425463800|223.68.184.0/23|56046|56046|STABLE
1425463800|81.3.170.248/32|65170|65170|STABLE
1425463800|91.214.17.0/24|196758|196758|STABLE
[...]
1425463800|207.201.203.0/24||40805|NEWROUTED
1425463800|2a03:ff80:101::/48|57541||REMOVED
1425463800|212.44.92.144/32|65170||REMOVED
1425463800|2001:4bb0:f:23::/64||41497|NEWROUTED
```


#### `routed-space`

Outputs information about address space that is "routed".

Usage:
```
consumer usage: routed-space
       -w <window-size>      window size in seconds (default 86400)
       -o <path>             output folder (default: current folder)
```

#### `triplets`

Output information about unique "triplets" seen in views.

Usage:
```
consumer usage: triplets
       -w <window-size>      window size in seconds (default 604800)
       -o <output-folder>    output folder (default: ./)
```

### IODA Consumers

#### `per-as-visibility`

Tracks per-AS statistics about prefixes announced on BGP.

Usage:
```
consumer usage: per-as-visibility
```

#### `per-geo-visibility`

Tracks per-geo statistics about prefixes announced on BGP.

Usage:
```
consumer usage: per-geo-visibility -p <ipmeta-provider>
```

### Test/Template Consumers

#### `test`

Simple testing consumer.

Usage:
```
consumer usage: test
```

#### `myviewprocess`

Template consumer that can be copied to start development of a new consumer.

### Misc Consumers
 - `pathchange`
 - `perfmonitor`


## Writing a New Consumer

TODO (kkeys)
