
# Summarize (the 10% subset of) the kdd99 data set. This should be the first step in any analysis you do using ML: the
# summary file contains information on the distribution of all of the numerical and categorical variables in your dataset,
# and this summary information is required to configure the MapReduce jobs run by the other tools.
#
# ML is designed to work with CSV-style data as well as Vectors (stored either as Mahout's VectorWritables in SequenceFiles
# or using our own MLVector Avro format). By convention, commandline arguments that end in "path" or "paths" refer to data
# that is stored (or will be stored) in HDFS, whereas commandline arguments that end in "file" or "files" refer to data that
# is stored (or will be stored) on the client machine. In the summary command, we are reading data from HDFS via the
# --input-paths argument, and storing the summary information we find locally in the location specified by the --summary-file
# argument.
#
# For CSV data, the summary command needs a CSV --header-file that tells it the name of each of the columns in the dataset and
# their data type. Once HCatalog integration is in place, we will pull this information in automatically. You can also specify
# that certain fields in the dataset should be ignored by the summarizer by marking their type as "ignored" or "id".
#
# The summary command runs a single MapReduce job every time it is called.
client/bin/ml summary --input-paths kddcup.data_10_percent_corrected --format text --header-file examples/kdd99/header.csv \
  --summary-file examples/kdd99/s.json

# Use the summary information that we just created to "normalize" the kdd99 data, which in this context, means converting it
# from a CSV file into a collection of Vectors. This command supports writing the data out as either VectorWritables stored
# in a SequenceFile, or as MLVectors stored in an Avro file, depending on the value of the --output-type argument.
#
# By default, the normalize command converts every level of each of the categorical variables in a dataset into multiple 0/1
# indicator variables, one for each level. You can also use the --transform command to transform the values of the numerical
# variables, either using the Z transform (mean 0, s.d. of 1) or a linear transform (normalized to be between 0 and 1.)
#
# If you specify a column name or index via the --id-column argument, ML will store that identifier with each record and
# persist it for later analysis of the data. This is almost always a good idea.
#
# The normalize command runs a single map-only job every time it is called.
client/bin/ml normalize --input-paths kddcup.data_10_percent_corrected --format text --summary-file examples/kdd99/s.json \
  --transform Z --output-path kdd99 --output-type avro --id-column category

# Processes the normalized vectors and runs a series of MapReduce jobs over the data that are designed to construct a good
# set of sample points to use as the input to a locally run k-means clustering algorithm. The implementation of this algorithm
# follows the description in the "Scalable k-means++" paper by Bahmani et al. (2012) but has a few optimizations in order to
# reduce the number of passes over the data required to create the sample, or "sketch."
#
# This command will partition the input data set into two folds and will construct a sketch of each of them by
# sampling --points-per-iteration points from each fold over N iterations (controlled by the --iterations argument). The
# reason for creating two folds instead of just one is that this allows us to compute a number of statistics related to the
# stability of the clustering that we can use to help us choose a good value of K from the sketches.
#
# The ksketch commands will run N + 1 MapReduce jobs, where N is controlled by the --iterations argument.
client/bin/ml ksketch --input-paths kdd99 --format avro --points-per-iteration 500 --output-file wc.avro --seed 1729 \
  --iterations 5 --cross-folds 2

# Runs k-means++ on the sketch that was created by the ksketch command for multiple values of K.
#
# This command does not run on Hadoop at all, all of the computations take place on the client machine. The goal is to
# allow a data scientist to experiment with multiple values of K and multiple clusterings without requiring them to run
# additional MapReduce jobs. That said, performing these calculations can be very compute-intensive, so you may want to
# run this command on a more powerful machine.
#
# If you used multiple folds in the ksketch command, the kmeans command will use them to create both 'test' and 'train'
# clusterings over the different folds and evaluate the stability of the resulting cluster assignments and print out
# statistics about the clusterings. The values that are important for evaluating stability are: 'PredStrength', based on a
# modified version of the prediction strength calculation described in "Cluster Validation by Prediction Strength" by
# Tibshirani et al. (2005); 'StableClusters', which is the fraction of the K clusters that were deemed stable by the
# prediction strength algorithm; and 'StablePoints', which is the fraction of the total points in the test set that belonged
# to a stable cluster.
#
# For small values of K on well-clustered data, you should aim to use the largest value of K whose prediction strength exceeds
# 0.8. For larger values of K on data that is not well-clustered, you will have more small clusters that tend to be less stable,
# which will lower the prediction strength metric, and should try to weigh both the cost of the clustering (which will always
# decrease with larger K) as well as the StablePoints fraction (ideally > 90%) in choosing a value of K.
#
# Finally, you can use the --best-of parameter to perform multiple iterations of k-means++ with different starting points for
# each of your values of K in the --clusters argument.
client/bin/ml kmeans --input-file wc.avro --centers-file centers.avro --seed 19 --clusters 1,10,20,30,40,50 --best-of 2

# Take the output centers created by the kmeans command and apply them to input data in order to analyze how points were
# assigned to different clusters. The output of this command is a CSV file with four fields:
#
# 1. ID (The ID associated with the vector, the one that you associated with it via the normalize command)
# 2. Clustering ID (You can apply multiple clusterings to the data on each run via the --centers-file and --center-ids)
# 3. Center ID (Which of the clusters this point was assigned to in the current clustering)
# 4. Distance (How far this point was from its assigned center)
#
# This output data can then be analyzed using Impala or any other analytical framework you like in order to study how the
# points clustered and quickly identify the anomalous events in your data (i.e., small, unstable clusters and points that
# ended up far from their assigned center.)
client/bin/ml kassign --input-path kdd99 --format avro --centers-file centers.avro --center-ids 4 --output-path assigned
