from pyspark import SparkConf, SparkContext
import sys
import string


inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count improved')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+
#20160801-020000 en AaagHiAag 1 8979
def words_once(line):

    record = line.split()

    record_tuple_value = (record[0], record[1], record[2], int(record[3]), record[4])

    return record_tuple_value


def get_key(kv):
    return kv[0]


def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])


text = sc.textFile(inputs)
records = text.map(words_once)
filtered_records = records.filter(lambda n: n[1] == "en").\
    filter(lambda n: n[2] != "Main_Page" and not n[2].startswith("Special:"))

wordcount = filtered_records.map(lambda n: (n[0], (n[3], n[2]))).reduceByKey(lambda x, y: x if x[0] > y[0] else y)

outdata = wordcount.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)