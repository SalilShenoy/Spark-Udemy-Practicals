from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Breadth First Search')
sc = SparkContext(conf = conf)

startCharacterId = 5306 #SpiderMan
targetCharaterId = 14 #ADAM 3031

hitCounter = sc.accumulator(0)


def convertToBFS(line):
    fields = line.split()
    heroId = fields[0]
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if (heroId == startCharacterId):
        color = 'GRAY'
        distance = 0

    return heroId, (connections, distance, color)


def createStartingRDD():
    inputfile = sc.textFile('Datasets/Marvel-Graph.txt')
    return inputfile.map(convertToBFS)


def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharaterId == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        color = 'BLACK'

    results.append((characterID, (connections, distance, color)))
    return results


def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = 'WHITE'
    edges = []

    if (len(edges1) > 0):
        edges = edges1
    elif (len(edges2) > 0):
        edges = edges2

    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    return (edges, distance, color)


iterationRDD = createStartingRDD()

for iteration in range(0, 10):
    print ('Running BFS iteration # ' + str(iteration))

    mapped = iterationRDD.flatMap(bfsMap)

    print('Processing ' + str(mapped.count()) + ' values')

    if (hitCounter.value > 0):
        print('Hit the target character from ' + str(hitCounter.value) + ' different directions')
        break

    iterationRDD = mapped.reduceByKey(bfsReduce)