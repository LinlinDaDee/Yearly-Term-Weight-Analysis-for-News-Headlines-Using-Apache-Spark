from operator import add

from pyspark import SparkContext, SparkConf
import sys
import math

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col, lit
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, BooleanType, DoubleType


class Project2:
        
    def run(self, inputPath, outputPath, stopwords, k):
        spark = SparkSession.builder.master("local[*]").appName("project2_df").getOrCreate()

        # parse the year and terms
        def extract_year_headline(year,terms):
            year = year[0:4]
            if terms == None or len(terms) == 0:
                return (year,[(year,'_empty_headline',1, 1)])
            terms = terms.split(' ')
            term_frequency = {}
            for term in terms:
                if term in term_frequency.keys():
                    term_frequency[term] = term_frequency[term] + 1
                else:
                    term_frequency[term] = 1
            return (year,[(year,term,1,term_frequency[term]) for term in set(terms)])


        structType = StructType([
            StructField("year", StringType(), True),
            StructField("struct", ArrayType(
                StructType(
                    [
                        StructField("year", StringType(), True),
                        StructField("term", StringType(), True),
                        StructField("ni", IntegerType(), True),
                        StructField("tfij", IntegerType(), True)
                    ]
                )
            ), True)
        ])


        extract_year_headline_udf = udf(extract_year_headline,structType)

        df = spark\
            .read\
            .option('header', False)\
            .option('delimiter', ',')\
            .csv(inputPath)\
            .toDF('year', 'terms')\
            .select(extract_year_headline_udf(col('year'),col('terms')).alias('line'))\
            .cache()
        # calculate the number of headlines in y
        N_df = df.select(df.line.year.alias('year')).groupby('year').count().toDF('year', 'N')

        df2 = df\
            .select(df.line.struct.alias('struct'))\
            .select(explode(col('struct')).alias('struct'))\
            .select(col('struct').year.alias('year'),col('struct').term.alias('term'),col('struct').ni.alias('ni'),col('struct').tfij.alias('tfij'))

        # Calculate the frequency of t in y (ni) and TF
        df3 = df2.groupby('year','term').sum('ni','tfij').toDF('year','term','sum_ni','sum_tfij')

        df3.filter(" term == 'fire' ").show()
        # top-n terms
        stopwords_set = df3\
            .groupby('term')\
            .sum('sum_ni')\
            .toDF('term','term_total_count')\
            .orderBy(col('term_total_count').desc(),col('term'))\
            .rdd\
            .map(lambda x:x['term'])\
            .take(int(stopwords))

        broad_stopwords_set = spark.sparkContext.broadcast(stopwords_set)
        # filter terms
        def filter_terms(term):
            if term in broad_stopwords_set.value:
                return False
            return True

        filter_terms_udf = udf(filter_terms,BooleanType())
        df4 = df3.filter(filter_terms_udf(df3.term))

        #calculate weight
        def calc_weight(sum_tfij,sum_ni,N):
            tf = math.log10(sum_tfij)
            idf = math.log10(N / sum_ni)
            weight = tf * idf
            return weight

        calc_weight_udf = udf(calc_weight,DoubleType())
        df5 = df4\
            .join(N_df,'year')\
            .select(col('term'),calc_weight_udf(col('sum_tfij'),col('sum_ni'),col('N')).alias('weight'),lit(1).alias('w_count'))

        def format_result(term, weight):
            formated_string = '"%s"\t%.9f' % (term, weight)
            return formated_string

        format_result_udf = udf(format_result, StringType())

        result = df5\
            .groupBy('term')\
            .sum('weight','w_count')\
            .toDF('term','weight','w_count')\
            .select(col('term'),(col('weight') / col('w_count')).alias('weight_avg'))\
            .orderBy(col('weight_avg').desc(),col('term'))\
            .limit(int(k))
        #output
        result.select(format_result_udf(col('term'), col('weight_avg'))) \
            .write\
            .text(outputPath)

        # Stopping Spark context
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])


