from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import time
import sys
import binascii


port = int(sys.argv[1])
output_file = sys.argv[2]

filter_array = [0] * 200
actual_set = set()
false_positive = 0
batch_round = 0


def bloom_filter(t, rdd):
    print("Time: %s" % t)
    print("count: " + str(rdd.count()))

    city_list = rdd.collect()
    for city in city_list:
        estimate_in_set = True

        hash_1 = int(binascii.hexlify(city.encode('utf8')), 16) % 200
        hash_2 = hash(city) % 200
        hash_3 = (int(binascii.hexlify(city.encode('utf8')), 16) * 3 + 19) % 200

        hash_set = set()
        hash_set.add(hash_1)
        # hash_set.add(hash_2)
        # hash_set.add(hash_3)
        # print("hash set count: " + str(len(hash_set)))
        # print(str(city) + ": " + str(hash_set))

        for index in hash_set:
            if filter_array[index] != 1:
                estimate_in_set = False
                break

        # count false positive
        if estimate_in_set and (city not in actual_set):
            global false_positive
            false_positive += 1
            # print("false positive: " + str(city))
            # print("set: " + str(actual_set))

        # update filter for next round
        for index in hash_set:
            filter_array[index] = 1
        actual_set.add(city)

        counter = 0
        for x in filter_array:
            if x == 1:
                counter += 1
        print("count 1's: " + str(counter))

    global batch_round
    batch_round += 1
    print("actual set count: " + str(batch_round) + ", " + str(len(actual_set)))
    print("false positive: " + str(false_positive))
    print("false positive rate: " + str(false_positive / len(actual_set)))

    with open(output_file, "a") as fp:
        fp.write("%s" % t + "," + str(false_positive / len(actual_set)))
        fp.write("\n")


sc = SparkContext('local[*]', 'task1')

start = time.time()

with open(output_file, "w") as fp:
    fp.write("Time,FPR\n")

ssc = StreamingContext(sc, 10)
dstream = ssc.socketTextStream("localhost", port)
parsedRDD = dstream.map(lambda line: json.loads(line))
cityRDD = parsedRDD.map(lambda arr: arr["city"])
cityRDD.foreachRDD(lambda t, rdd: bloom_filter(t, rdd))


# start streaming
ssc.start()
# stop when the socket we are listening is dead
ssc.awaitTermination()

end = time.time()
print('Duration: ' + str(end - start))
