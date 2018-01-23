from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('MOst Popular Movies')
sc = SparkContext(conf =  conf)


def reverse_map(x, y):
    return y, x


movie_data = sc.textFile('Datasets/ml-100k/u.data')
movies = movie_data.map(lambda x: (int(x.split()[1]), 1))
movie_counts = movies.reduceByKey(lambda x, y: x + y)
count_movies = movie_counts.map(lambda x_y: (x_y[1], x_y[0]))
sorted_by_count = count_movies.sortByKey()
results = sorted_by_count.collect()

for result in results:
    print(result)