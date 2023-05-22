from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('input_file', help='path to input file')
parser.add_argument('output_file', help='path to output file')

args = parser.parse_args()

spark = (SparkSession
	.builder
	.appName("SortRows")
	.getOrCreate())

# Read data from file into a dataframe
df = spark.read.csv(args.input_file)

# sort on the 3rd and last column
out_df = df.sort(['_c2', '_c14'], ascending=True)

# write the output as a single file
out_df.coalesce(1).write.csv(args.output_file)
