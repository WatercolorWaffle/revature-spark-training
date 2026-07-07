from pyspark import SparkConf, SparkContext
from prettytable import PrettyTable

conf = SparkConf().setMaster('local').setAppName('MaxTemps')
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])
    return (stationID, entryType, temperature)

lines = sc.textFile('./weatherData1800s.csv')
parsedLines = lines.map(parseLine)
maxTemps = parsedLines.filter(lambda x: 'TMAX' in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()

table = PrettyTable()
table.field_names = ['Station ID', 'Max Temperature']

for id, temp in results:
    table.add_row([id, temp])

print(table)
