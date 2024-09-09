from pyspark import SparkContext, SparkConf
from pprint import pp
# conf = SparkConf().setAppName("deb-spark").setMaster("local[*]")
sc = SparkContext(master="local",appName="deb-spark")

def read_file():
    rdd = sc.textFile("c:/test/first.txt")

    for ln in rdd.collect():
        pp(ln)
isEven = lambda x: x%2==0        
def filter_rdd():
    rdd = sc.parallelize([5,8,9,10,11,56])
    rdd2=rdd.filter(isEven)
    pp([ln for ln in rdd2.collect()])
        

    
if __name__ == "__main__":
    filter_rdd()
    