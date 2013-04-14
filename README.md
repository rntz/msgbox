# What is msgbox?

msgbox is a distributed push-notification system that persists notifications to
disk so that you can receive notifications the next time you connect if you
happen to be offline when they happen.

That's what it's supposed to be, anyway. It's not quite finished yet, though it
does seem to work, as of 2013-04-14.

I'm making it because I want an easy way to be notified if and when, eg.,
someone pings me on IRC. It could also be used as the basis for, say,
synchronizing a TODO-list across multiple machines. TODO: explain how.

It's "distributed" in that it doesn't have a central point of failure; every
node running `msgbox-peer` keeps as much information as it can get, and
propagates it to as many other nodes as it can reach. However, it's not (yet?)
smart enough to figure out network topology for itself; you have to tell it what
other nodes to try to connect to. The way it propagates notifications is also
really stupid and redundant and nonperformant if you have a large and/or
high-degree network graph.

I have a tiny network (~4 machines, tops), so these issues don't affect me.
Patches thoughtfully considered.

# How does it work?

TODO
