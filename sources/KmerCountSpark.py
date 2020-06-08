import sys
from pyspark import SparkContext, SparkConf
import time
if __name__ == "__main__":
    sc = SparkContext("local", "Kmer Counting")
    genome = ""
    kmers = []

    start = time.time()
    
    inputData = sc.textFile("./ecoli.fa/ecoli.fa").collect()
    for line in inputData:
        genome += line
    for i in range(0, len(genome) - 9 + 1):
        kmers.append(genome[i:i+9])
    kmers = sc.parallelize(kmers)
    kmerCount = kmers.map(lambda kmer: (kmer, 1)).reduceByKey(lambda a, b: a + b)
    kmerCount.saveAsTextFile('./output/')

    end = time.time()
    #add new comment
    with open('./output/time.txt', 'w') as f:
        f.write(str(end-start))