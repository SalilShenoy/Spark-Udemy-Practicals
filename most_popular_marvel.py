from pyspark import SparkConf, SparkContext


def createDict(line):
    fields = line.split('\"')
    return(int(fields[0]), fields[1].encode('utf-8'))


def createCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)


conf = SparkConf().setMaster('local').setAppName('Most Popular Marvel Map')
sc = SparkContext(conf = conf)

marvel_names = sc.textFile('Datasets/Marvel-Names.txt')
marvel_names_rdd = marvel_names.map(createDict)

social_graph = sc.textFile('Datasets/Marvel-Graph.txt')
co_occurances_rdd = social_graph.map(createCoOccurences)
total_friends = co_occurances_rdd.reduceByKey(lambda x, y: x+y)
friends_marvel_hero = total_friends.map(lambda x_y: (x_y[1], x_y[0]))

most_popular = friends_marvel_hero.max()

most_popular_name = marvel_names_rdd.lookup(most_popular[1])[0]

print (most_popular_name.decode('utf-8') + ' is the most popular marvel hero, with ' + str(most_popular[0]) + ' co-appearances')
