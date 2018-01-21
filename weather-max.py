from pyspark import SparkConf, SparkContext


def parse_lines(line):
    fields = line.split(',')
    station_name = fields[0]
    temp_type = fields[2]
    temp = float(fields[3]) * 0.1
    return station_name, temp_type, temp


conf = SparkConf().setMaster('local').setAppName('Max Temp')
sc = SparkContext(conf = conf)
weather_data = sc.textFile('Datasets/1800.csv')
lines = weather_data.map(parse_lines)
max_temp = lines.filter(lambda x: 'TMAX' in x[1])
station = max_temp.map(lambda x: (x[0], x[2]))
max_temps = station.reduceByKey(lambda x,y: max(x,y))
results = max_temps.collect()
for result in results:
    print(result[0] + ' ' + str(result[1]))