from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('friends-by-age')
sc = SparkContext(conf = conf)


def parse_lines(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age,num_friends


lines=sc.textFile('Datasets/fakefriends.csv')
rdd=lines.map(parse_lines)
totalsByAge=rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge=totalsByAge.mapValues(lambda x: x[0] / x[1])
results=averagesByAge.collect()
for result in results:
    print(result)