from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster('local').setAppName('Rating Counter')
sc = SparkContext(conf = conf)

lines = sc.textFile('Datasets/ml-100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sorted_results = collections.OrderedDict(sorted(result.items()))
for key, value in sorted_results.items():
    print('%s  %i' % (key, value))