from pyspark import SparkConf, SparkContext

def load_movie_names():
    movie_names = {}
    with open('Datasets/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


conf = SparkConf().setMaster('local').setAppName('Most Popular Movies')
sc = SparkContext(conf =  conf)

namedDict = sc.broadcast(load_movie_names())

lines = sc .textFile('Datasets/ml-100k/u.data')
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movie_count = movies.reduceByKey(lambda x, y: x + y)
counts_movie = movie_count.map(lambda x_y: (x_y[1],x_y[0]))
sorted_movies = counts_movie.sortByKey()

sorted_movie_names = sorted_movies.map(lambda count_movie : (namedDict.value[count_movie[1]], count_movie[0]))

results = sorted_movie_names.collect()

for result in results:
    print(result)
