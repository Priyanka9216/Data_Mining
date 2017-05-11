import os
import sys
import time
import itertools
from pyspark import SparkContext
from collections import defaultdict

# os.environ['SPARK_HOME'] = "/Users/priyankashah/Downloads/spark-2.1.0-bin-hadoop2.7"
# spark_home = os.environ.get('SPARK_HOME',None)
# sys.path.append("/Users/priyankashah/Downloads/spark-2.1.0-bin-hadoop2.7/python")
# sys.path.insert(0,os.path.join(spark_home,'python','lib','py4j-0.10.4-src.zip'))
sc = SparkContext(appName="Task2AvergaeRating")


def a_priori(iterator):
    original_basket = []
    partition_multiple_frequent_itemsets = set()

    for i in iterator:
        original_basket.append(i[1])
        # for j in i[1]:
        #     unique_set.add(j)


    new_support = toivonen_support
    candidate_single_frequent_itemsets = set()
    counting_pair = set()
    singleton_itemset_count_dict = {}

    #Create singleton frequent itemset
    for sub_basket in original_basket:
        for i in sub_basket:
            if singleton_itemset_count_dict.get(i) != None:
                singleton_itemset_count_dict[i] += 1
            else:
                singleton_itemset_count_dict[i] = 1

    for a in singleton_itemset_count_dict:
        if singleton_itemset_count_dict[a] >= new_support:
            candidate_single_frequent_itemsets.add(a)
            partition_multiple_frequent_itemsets.add((a,1))

    #Create pairs
    for subset in itertools.combinations(candidate_single_frequent_itemsets, 2):
        subset = set(subset)
        count = 0
        for i in original_basket:
            if subset.issubset(i):
                count += 1
        if count >= new_support:
            counting_pair.add(frozenset(subset))
            partition_multiple_frequent_itemsets.add((frozenset(subset),1))


    k = 3

    deeped_copied = counting_pair
    while(len(deeped_copied) != 0):

        print('items',k)
        print(len(deeped_copied))

        counting_multiple_pair = set()
        count_temp_set = set()
        comb_candidate_items = defaultdict(lambda: 0)
        number_of_combinations_kc2 = (k*(k-1))/2
        print(number_of_combinations_kc2)

        org_comb = itertools.combinations(deeped_copied,2)
        for sub in org_comb:
            seta = set().union(*sub)
            if len(seta) == k:
                s = ''
                for ir in sorted(seta):
                    s += str(ir)
                comb_candidate_items[s] += 1
                if comb_candidate_items[s] == number_of_combinations_kc2:

                    #count_temp_set.add(frozenset(seta))

                    counting_multiple_pair.add(frozenset(seta))

        for pair in counting_multiple_pair:
            count = 0
            for i in original_basket:
                if pair.issubset(i):
                    count += 1
            if count >= new_support:
                partition_multiple_frequent_itemsets.add((frozenset(pair),1))
                count_temp_set.add(frozenset(pair))

        # if present == False:
        #     deeped_copied.clear()
        #     break

        #deeped_copied.clear()
        deeped_copied = count_temp_set

        k += 1

    # print("This is Partition")
    # print(partition_multiple_frequent_itemsets)

    return iter(partition_multiple_frequent_itemsets)



def Phase2Count(iterator):
    phase2_itemset_count = set()
    basket_list = []
    for i in iterator:
        basket_list.append(i[1])
    for d in data1:
        count234 = 0
        if isinstance(d[0], int):
            for bas in basket_list:
                if d[0] in bas:
                    count234 += 1

        else:
            for bas in basket_list:
                if d[0].issubset(bas):
                    count234 += 1

        phase2_itemset_count.add((d[0],count234))
    return iter(phase2_itemset_count)



original_support = int(sys.argv[3])
time1 = time.time()
filename = sys.argv[2]
lines = sc.textFile(filename)
header = lines.first()
file1 = lines.filter(lambda x: x != header)
if sys.argv[1] == '1':
    data = file1.map(lambda x: (int(x.split(',')[0]), int(x.split(',')[1]))).groupByKey().mapValues(set)
elif sys.argv[1] == '2':
    data = file1.map(lambda x: (int(x.split(',')[1]), int(x.split(',')[0]))).groupByKey().mapValues(set)

numOfPartitions = data.getNumPartitions()
toivonen_support = original_support / numOfPartitions
data1 = data.mapPartitions(a_priori).reduceByKey(lambda a, b: a+b).collect() # End Of Phase1 Of Son's Algorithm. We get candidate Key Value Pair (using samller support) on each partition and we have combined all partitions result till this point.
#print(len(data1))

data2 = data.mapPartitions(Phase2Count).reduceByKey(lambda a,b: a+b).collect()
super_final_itemset_dict = defaultdict(lambda : [])

#Making Final Frequent Itemset Dictionary
for d in data2:
    if d[1] >= original_support:
        if isinstance(d[0], int):
            super_final_itemset_dict[1].append(d[0])
        else:
            super_final_itemset_dict[len(d[0])].append(sorted(d[0]))

#Printing in the output file
total_count = 0
fileout = open('Priyanka_Shah_SON_MovieLens.Big.case2-3000.txt', 'w')

for i in sorted(super_final_itemset_dict.keys()):
    pres = True
    for j in sorted(super_final_itemset_dict[i]):
        total_count += 1
        if isinstance(j,int):
            if pres:
                fileout.write('(' + str(j) + ')')
                pres = False
            else:
                fileout.write(',(' + str(j) + ')')
        else:
            in_pres = True
            list1 = ''
            for x in j:
                if in_pres:
                    list1 += str(x)
                    in_pres = False
                else:
                    list1 += ',' + str(x)
            if pres:
                fileout.write('(' + list1 + ')')
                pres = False
            else:
                fileout.write(',(' + list1 + ')')
    fileout.write('\n')
fileout.close()


# coun = 0
# for key in super_final_itemset_dict:
#     coun += len(super_final_itemset_dict[key])
#     print sorted(super_final_itemset_dict[key])



print("This is length of frequent Itemsets")
print(total_count)
# print(super_final_itemset)
print(time.time() - time1)


