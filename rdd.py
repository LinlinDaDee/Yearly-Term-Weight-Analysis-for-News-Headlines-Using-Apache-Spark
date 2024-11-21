from operator import add

from pyspark import SparkContext, SparkConf
import sys
import math

class Project2:   
        
    def run(self, inputPath, outputPath, stopwords, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)

        # Extract the year and terms
        def extract_year_headline(x):
            tokens = x.split(',')
            year = tokens[0][0:4]
            terms = tokens[1]
            # (2003,(2003,“councilchiefexecutivefailstosecureposition”))
            return (year,(year,terms))

        rdd = sc.textFile(inputPath).map(extract_year_headline)
        rdd.cache()


        #calculate the number of headlines in y
        #(2003,2)
        N_rdd = rdd.map(lambda x:(x[0],1)).reduceByKey(add)

        # Break the terms down into individual term
        def parse_terms(x):
            year = x[0]
            terms = x[1][1].split(' ')
            result_map = {}
            for term in terms:
                # {“2003,executive”,(2003,“executive”,1,1)}
                key = year + ',' + term
                if key in result_map.keys():
                    tup = result_map[key]
                    result_map[key] = (tup[0],tup[1],tup[2],tup[3] + 1)
                else:
                    result_map[key] = (year, term, 1, 1)
            result = []
            for key,value in result_map.items():
                result.append((key,value))
            return result
        rdd2 = rdd.flatMap(parse_terms)

        # for i in rdd2.collect():
        #     print(i)
        # Calculate the frequency of t in y (ni) and TF
        rdd3 = rdd2.reduceByKey(lambda x,y:(x[0],x[1],x[2] + y[2],x[3] + y[3]))
        rdd3.cache()

        # print('=======rdd3=========')
        # # {“2003,council”,(2003,“council”,2,2)}
        #
        # for i in rdd3.collect():
        #     print(i)
        # top-n terms
        stopwords_set = rdd3.map(lambda x:(x[1][1],x[1][3])).reduceByKey(add).sortBy(lambda x:x[1],ascending=False).map(lambda x:x[0]).take(int(stopwords))

        # print('=======stopwords_set=========')
        # print(stopwords_set)

        #filter terms
        rdd4 = rdd3.filter(lambda x:x[0].split(',')[1] not in stopwords_set)

        # print('=======rdd4=========')
        # for i in rdd4.collect():
        #     print(i)

        #calculate weight
        def calculate_weight(year,x):
            # the number of headlines in y
            term,number_headlines_in_y_having_t,frequency_t_in_y = x[0][1],x[0][2],x[0][3]

            number_headlines_in_y = x[1]
            tf = math.log10(frequency_t_in_y)
            idf = math.log10(number_headlines_in_y/number_headlines_in_y_having_t)
            weight = tf * idf
            return (term,(weight,1))

        rdd5 = rdd4.map(lambda x:(x[0].split(',')[0],x[1]))\
            .join(N_rdd)\
            .map(lambda x:calculate_weight(x[0],x[1]))

        result = rdd5.reduceByKey(lambda x,y:(x[0] + y[0],x[1] + y[1]))\
            .map(lambda x:(x[0],x[1][0] / x[1][1]))\
            .coalesce(1)\
            .sortByKey()\
            .sortBy(lambda x:x[1],ascending=False) \
            .map(lambda x: '"%s"\t%.9f' % (x[0], x[1]))\
            .take(int(k))

        sc.parallelize(result,numSlices=1).saveAsTextFile(outputPath)

        # Stopping Spark context
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])


