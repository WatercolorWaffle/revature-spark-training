from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local[*]').setAppName('DegreesOfSeparation')
sc = SparkContext(conf = conf)

startCharacterID = 5306
targetCharacterID = 14

hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if(heroID) == startCharacterID:
        color = 'GRAY'
        distance = 0

    return (heroID, (connections, distance, color))

def createStartingRdd():
    inputFile = sc.textFile('./MarvelGraph.txt')
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterID = node[0]
    data = node[1]

    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if(color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'

            if(newCharacterID == targetCharacterID):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        color = 'BLACK'

    results.append((characterID, (connections, distance, color)))
    return results


    

iterationRdd = createStartingRdd()

for iteration in range(0,10):
    print('Running BFS iteration# ' + str(iteration + 1))

    mapped = iterationRdd.flatMap(bfsMap)

    if(hitCounter.value > 0):
        print('Hit the target character! From ' + str(hitCounter.value) + ' different direction(s).')
        break

    iterationRdd = mapped.reduceByKey(bfsReduce)