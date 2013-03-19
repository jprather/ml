Introduction
------------
This is Cloudera ML, a set of Java libraries and commandline tools for performing certain data preparation
and analysis tasks that are often referred to as "advanced analytics" or "machine learning." Our focus is
on simplicity, reliability, easy model interpretation, minimal parameter tuning, and integration with other
tools for data preparation and analysis.

We're kicking things off by introducing a set of tools for performing scalable k-means clustering on
Hadoop. We will expand the set of model fitting algorithms we support over time, but our primary focus will
be on data preparation and model evaluation.

Getting Started
---------------
To run this package on your machine, you should first run:

	mvn clean install

There is a script in the `client/bin` directory named "ml" that can be used to run the commands
that this library supports. Run `client/bin/ml help` to see the list of commands and
`client/bin/ml help <cmd>` to get detailed help on the arguments for any individual command.

If you would like to pack everything up and carry it around with you, running

	tar -cvzf ml.tar.gz client/bin/ml client/target/ml-client-0.1.0.jar client/target/lib/

will create a handy little archive with everything you need.

An Example Workflow
-------------------
The `examples/kdd99` directory contains an annotated workflow that describes the process of finding clusters
in some data from [KDD Cup '99](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html), a publicly available
dataset that is widely used as a reference for evaluating clustering algorithms for anomaly detection.

Some Obvious TODOs
------------------
A handful of high-priority items for the old TODO list, in no particular order:

* HCatalog integration
* Javadoc-ing, well, everything
* Commandline support for the pivot table operators
