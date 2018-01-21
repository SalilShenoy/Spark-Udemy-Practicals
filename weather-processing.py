from  pyspark import SparkConf, SparkContext


def parse_lines(line):
    fields = line.split(',')
    station = fields[0]
    temp = float(fields[3]) * 0.1
    type = fields[2]
    return station, type, temp


conf = SparkConf().setMaster('local').setAppName('weather-processor')
sc = SparkContext(conf = conf)
weather_data = sc.textFile('Datasets/1800.csv')
lines = weather_data.map(parse_lines)
min_temp = lines.filter(lambda x: 'TMIN' in x[1])
stations_temp = min_temp.map(lambda x: (x[0], x[2]))
minTemps = stations_temp.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect()
for result in results:
    print( result[0] + ' ' + str(result[1]))
