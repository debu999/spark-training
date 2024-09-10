# spark-training
rpsconsulting spark training code

- sample command: `spark-submit --master "local[*]" sparkcore\spark_map.py`

- important diff: `sortBy` and `sortByKey`

- spark shuffle partition/repartition trigger new stages.
- executors are created when a job is submitted.

>>> df = spark.read.option("multiLine", True).json("C:/spark-training/samples/sample.json")
>>> df.show()

+----+-------+-----+
| age|   name|pcode|
+----+-------+-----+
|null|  Alice|94304|
|  30|Brayden|94304|
|  19|  Carla|10036|
|  46|  Diana| null|
|null|Étienne|94104|
+----+-------+-----+

- `--master` to pass master in command instead of code. Code takes precedence.

>>> df_sel = df.select(["name", "pcode"])
>>> df_sel.show()
+-------+-----+
|   name|pcode|
+-------+-----+
|  Alice|94304|
|Brayden|94304|
|  Carla|10036|
|  Diana| null|
|Étienne|94104|
+-------+-----+