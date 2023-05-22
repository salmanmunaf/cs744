from pyspark.sql import SparkSession
import argparse
import re

def strip_category(word):
    """Strips the word 'category:' from the given word and returns it."""
    if word.startswith("category:"):
        return word[len("category:"):]
    return word

def fn_split_line(line):
    """Splits the line on space/tab and returns the first 2 elems in the list."""
    words = re.split(r'\t+', line)
    w1 = strip_category(words[0].lower())
    w2 = strip_category(words[1].lower())
    return w1, w2

def calc_contribution(rank, urls):
    """Calculates the contribution of a node to all its neighbors."""
    # Suppose A has edges with B, C and D, then the contribution to each of
    # B, C, and D from A will be rank(A)/#number of links for A, i.e rank(A)/3
    ll = len(urls)
    for url in urls:
        yield (url, rank/ll)


def validate_input(line):
    """Filters lines not matching the expected input format"""
    # if line has 0 length, or starts with a comment, filter it.
    if len(line) == 0 or line[0] == '#':
        return False

    # if after splitting by \t+, it has len less than 2, then filter it.
    words = re.split(r'\t+', line)
    if len(words) < 2:
        return False

    # filter edges with ':', but not the ones with 'Category:'
    if ":" in words[1] and not words[1].startswith("Category:"):
        return False

    if ":" in words[0] and not words[0].startswith("Category:"):
        return False

    return True

parser = argparse.ArgumentParser()
parser.add_argument('input_file', help='path to input file')
parser.add_argument('output_file', help='path to output file')
parser.add_argument('num_partitions', help='number of partitions')

args = parser.parse_args()

spark = (SparkSession \
        .builder \
        .appName("PageRankPartition") \
        .getOrCreate())

# forms an RDD with each line in the input file as a key
text_file_rdd = spark.read.text(args.input_file).rdd.map(lambda line: line[0])

# forms the edges by collecting all A, x pairs and forming a KV pair
# with key as A and value as a list of nodes.
edges = text_file_rdd.filter(lambda line: validate_input(line)) \
                    .map(lambda line: fn_split_line(line)) \
                    .distinct() \
                    .groupByKey()

# partition edges into num partitions as input by the user
edges = edges.partitionBy(int(args.num_partitions))
# use mapValues to preserver the parent partitioning scheme
ranks = edges.mapValues(lambda neighs: 1.0)

# run the pagerank algorithm for 10 iterations
for i in range(1, 11):
    new_ranks = edges.join(ranks) \
                    .flatMap(lambda edge_rank_info: calc_contribution(edge_rank_info[1][1], edge_rank_info[1][0])) \
                    .reduceByKey(lambda c1, c2 : c1+c2)
    # need to partition again as join/flatMap doesn't preserve the partition
    ranks = new_ranks.mapValues(lambda rank: rank*0.85 + 0.15).partitionBy(int(args.num_partitions))

# save the final pagerank RDD
ranks.saveAsTextFile(args.output_file)
