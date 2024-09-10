from pyspark import SparkContext, SparkConf
from pprint import pp
from time import sleep

# conf = SparkConf().setAppName("deb-spark").setMaster("local[*]")
# sc = SparkContext(master="local", appName="deb-spark")
sc = SparkContext(appName="deb-spark")
sc._jsc.hadoopConfiguration().set(
    "mapreduce.input.fileinputformat.input.dir.recursive", "true"
)


def read_file():
    rdd = sc.textFile("c:/test/first.txt")

    for ln in rdd.collect():
        pp(ln)


isEven = lambda x: x % 2 == 0


def filter_rdd():
    rdd = sc.parallelize([5, 8, 9, 10, 11, 56])
    rdd2 = rdd.filter(isEven)
    print_rdd(rdd2)
    rdd3 = rdd.filter(lambda n: n % 2 == 0)
    print_rdd(rdd3)
    print_rdd(rdd.filter(lambda n: n >= 10))
    print_rdd(rdd.filter(lambda n: n >= 10).map(lambda x: x * x))


def print_rdd(rdd):
    pp([ln for ln in rdd.collect()])


def text_rdd_operations():
    linesRdd = sc.textFile("c:/spark-training/samples")
    pp(linesRdd.collect())
    pp(
        linesRdd.filter(lambda line: line.startswith("s"))
        .map(lambda line: line.upper())
        .collect()
    )
    pp(
        linesRdd.filter(lambda line: not line.startswith("s"))
        .map(lambda line: line.upper())
        .collect()
    )


def text_rdd_op2():
    linesRdd = sc.textFile("c:/spark-training/samples")
    wordsRdd = linesRdd.map(lambda line: line.split(" ")).collect()
    pp(wordsRdd)
    wordsRddFlat = linesRdd.flatMap(lambda line: line.split(" ")).collect()
    pp(wordsRddFlat)


def number_rdd_op1():
    rdd = sc.parallelize([5, 8, 9, 10, 11, 56, 12, 45])
    pp(rdd.map(lambda x: x * 2).collect())
    # pp(rdd.flatMap(lambda x: x * 2).collect()) error
    pp(rdd.flatMap(lambda x: list([x * 2, x, x / 2])).collect())  # error


def pair_rdd_op1():
    pair_rdd1 = sc.parallelize([("a", 5), ("b", 12), ("a", 6), ("c", 3), ("b", 5)])
    pair_rdd2 = pair_rdd1.groupByKey()
    pp([(k, list(v)) for k, v in pair_rdd2.collect()])

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    pp(rdd.reduce(lambda x, y: x + y))

    p_rdd = sc.parallelize([("a", 5), ("b", 12), ("a", 6), ("c", 3), ("b", 5)])
    pp(p_rdd.reduceByKey(lambda x, y: x + y).collect())


def wordcount():

    lines_rdd = sc.textFile("c:/spark-training/samples")
    word_count = (
        lines_rdd.flatMap(lambda line: line.split(" "))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(ascending=False, keyfunc=lambda x: x[1])
    )

    pp(
        f"""word_count rdd: {word_count.collect()}
       have partitions {word_count.getNumPartitions()}."""
    )
    # word_count = word_count.repartition(10)
    # pp(f"new partitions : {word_count.getNumPartitions()}")
    # print(f"debug_string word_count : {word_count.toDebugString()}")


if __name__ == "__main__":
    # filter_rdd()
    # text_rdd_operations()
    # text_rdd_op2()
    # number_rdd_op1()
    # pair_rdd_op1()
    wordcount()
    sleep(300)
    sc.stop()
