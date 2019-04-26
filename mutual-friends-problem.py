#spark-submit C:\Users\ajink\Documents\cs547\hw1-bundle\hw1_1_spark.py C:\Users\ajink\Documents\cs547\hw1-bundle\q1\data\soc-LiveJournal1Adj.txt C:\Users\ajink\Documents\cs547\hw1-bundle\q1\output\1
import re
import sys
from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])
kv = lines.map(lambda l: (l.split("\t",1)[0], l.split("\t",1)[1].split(",")) if len(l.split("\t"))>1 else (l.split("\t",1)[0],[])  )

kv_dict = kv.collectAsMap()

def pairsFromList(listOfFriends,friend):
    returnList = []
    if listOfFriends == []:
        returnList.append((friend,(None,None)))
    for i in listOfFriends:
        for j in listOfFriends:
            if(i!=j and not(i in kv_dict[j])):
                returnList.append((i,(j,friend)))
    return returnList

kv1 = kv.flatMap(lambda l:pairsFromList(l[1],l[0]))

kv2 = kv1.map(lambda l: ( (l[0],l[1][0]), [l[1][1]] ) )

kvr1 = kv2.reduceByKey(lambda l1,l2:l1+l2).map( lambda l: ( l[0][0], [(l[0][1],len(l[1]))]) )

kvr2 = kvr1.reduceByKey(lambda l1,l2:l1+l2).map(lambda l: (l[0], sorted(l[1], key = lambda x: x[1], reverse=True ) ))

def detup(lst):
    ret_lst = []
    a=0
    for i in lst:
        a=a+1
        if(a>10):
            break
        ret_lst.append(i[0])
    return ret_lst

kvr3 = kvr2.map(lambda l:(l[0],detup(l[1]))).map(lambda l: l[0]+"\t" +",".join(filter(None,l[1])) )

kvr3.saveAsTextFile(sys.argv[2])
sc.stop()
