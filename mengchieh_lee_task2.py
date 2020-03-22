from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import time
import sys
import binascii


port = int(sys.argv[1])
output_file = sys.argv[2]


def count_unique(t, rdd):
    print("Time: %s" % t)
    print("count: " + str(rdd.count()))

    actua_set = set()
    city_list = rdd.collect()
    # max_trailing_0 = 0
    max_training_list = [0] * 6

    for city in city_list:
        actua_set.add(city)
        hash_1 = int(binascii.hexlify(city.encode('utf8')), 16)
        hash_2 = hash(city)
        hash_3 = int(binascii.hexlify(city.encode('utf8')), 16) * 3 + 7
        hash_4 = int(binascii.hexlify(city.encode('utf8')), 16) * 7 + 11
        hash_5 = hash(city) * 11 + 9
        hash_6 = hash(city) * 13 + 7
        hash_list = list()
        hash_list.append(hash_1)
        hash_list.append(hash_2)
        hash_list.append(hash_3)
        hash_list.append(hash_4)
        hash_list.append(hash_5)
        hash_list.append(hash_6)

        for i in range(len(hash_list)):
            hash_a = hash_list[i]
            hash_bin = bin(hash_a)
            trailing_count = 0
            for j in range(len(hash_bin) - 1, 0, -1):
                if hash_bin[j] != '0':
                    break
                trailing_count += 1
            max_training_list[i] = max(max_training_list[i], trailing_count)

        # for hash_a in hash_list:
        #     hash_bin = bin(hash_a)
        #     trailing_count = 0
        #     for i in range(len(hash_bin) - 1, 0, -1):
        #         if hash_bin[i] != '0':
        #             break
        #         trailing_count += 1
        #     max_trailing_0 = max(max_trailing_0, trailing_count)
        #     print("max_trailing_0 = max(" + str(max_trailing_0) + "," + str(trailing_count) + ")")

    estimation_list = list()
    for i in range(3):
        estimation_sum = 0
        for j in range(2):
            estimation_sum += pow(2, max_training_list[i * 2 + j])
        estimation_avg = estimation_sum / 2
        estimation_list.append(estimation_avg)

    estimation_list.sort()
    estimation_count = estimation_list[1]

    print("actual set count: " + str(len(actua_set)))
    # print("estimation count: " + str(pow(2, max_trailing_0)))
    print("estimation count: " + str(estimation_count))

    with open(output_file, "a") as fp:
        fp.write("%s" % t + "," + str(len(actua_set)) + "," + str(estimation_count))
        fp.write("\n")


sc = SparkContext('local[*]', 'task1')

start = time.time()

with open(output_file, "w") as fp:
    fp.write("Time,Ground Truth,Estimation\n")

ssc = StreamingContext(sc, 5)
dstream = ssc.socketTextStream("localhost", port)
parsedRDD = dstream.map(lambda line: json.loads(line)).window(30, 10)
cityRDD = parsedRDD.map(lambda arr: arr["city"])
cityRDD.foreachRDD(lambda t, rdd: count_unique(t, rdd))

# start streaming
ssc.start()
# stop when the socket we are listening is dead
ssc.awaitTermination()

end = time.time()
print('Duration: ' + str(end - start))
