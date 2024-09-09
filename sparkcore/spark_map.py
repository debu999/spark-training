from pyspark import SparkContext, SparkConf
from pprint import pp


# conf = SparkConf().setAppName("deb-spark").setMaster("local[*]")
sc = SparkContext(master="local", appName="deb-spark")
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


if __name__ == "__main__":
    # filter_rdd()
    text_rdd_operations()
