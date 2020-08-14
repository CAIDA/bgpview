# BGPView

BGPView is a set of highly optimized data structures and libraries for
(re-)construction, transport and analysis of BGP "routing tables".

## Background

For a detailed description of BGPView, see Section 6.2 of the
[BGPStream paper](https://www.caida.org/publications/papers/2016/bgpstream/bgpstream.pdf).

At a high level, the goal of BGPView is to facilitate the inference of
"Global" routing tables at a finer granularity than the RIB dumps
currently provided by the RouteViews and RIPE RIS projects. For
example, currently RouteViews collectors export a snapshot of their
RIB (a "RIB dump") every 2 hours -- for RIPE RIS this is once every 8
hours. For applications where latency matters (e.g., near-realtime
event detection), and/or are interested in observing short-duration
events, they cannot rely on the RIB dumps alone.

Normally the solution to this problem is to write some code that
starts with a RIB dump, and then incrementally applies update
information to approximate the state of each peer's routing table at
any point in time. Then, depending on the application, one can either
react to specific events (e.g., a prefix is announced, withdrawn,
etc.) or, periodically walk through these routing tables and perform
analysis on the entire "global" routing table. BGPView is designed to
make it simple to write analysis code in this latter model, where all
of the details of obtaining the raw BGP data, processing it, and
inferring the routing table for each peer are abstracted away from the
user. The user is instead able to focus on writing an analysis kernel
(a "BGPView Consumer") that is called every time a new inferred RIB (a
"view") is available.

There are two primary modes of operation for BGPView: realtime, and
offline.

In realtime mode, there is a set of distributed processes that obtain
the BGP data (using [BGPStream](https://bgpstream.caida.org)) as soon
as it is made available by the upstream projects, process it, and then
periodically (every 5 min in CAIDA's reference deployment) publish
fragments of each view to a Kafka cluster. Users can then connect
their analysis modules to this cluster using the BGPView toolset, and
as soon as a view becomes available, their code will analyze it.

In offline mode...

Quick Start
-----------

TODO

