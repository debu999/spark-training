from pyspark import SparkContext, SparkConf
from pprint import pp

# conf = SparkConf().setAppName("deb-spark").setMaster("local[*]")
sc = SparkContext(master="local", appName="deb-spark")


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
    print_rdd(rdd1.filter(lambda n: n >= 10))


def print_rdd(rdd):
    pp([ln for ln in rdd.collect()])


if __name__ == "__main__":
    filter_rdd()
