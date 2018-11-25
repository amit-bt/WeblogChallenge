from pyspark.sql.functions import *
from pyspark import SparkContext,SQLContext,SparkConf, HiveContext
from pyspark.sql.types import *
from pyspark.sql.window import *
import datetime
import os
import sys

if __name__ == '__main__':

    sc = SparkContext()
    hc = HiveContext(sc)
    mode = sys.argv[1]
    
    if mode == 'cluster':
        rddstg1 = sc.textFile('sample.log').flatMap(lambda line: line.split('\n')).map(lambda row: row.split('" '))
    else:
        rddstg1 = sc.textFile('/user/xamikuma/sample.log').flatMap(lambda line: line.split('\n')).map(lambda row: row.split('" '))
    
    rddstg2 = rddstg1.map(lambda cold: (cold[0].split('"'), cold[1], cold[2].split(' ')))
    
    rddstg3 = rddstg2.map(lambda idn: [idn[0][0], idn[0][1], idn[1], idn[2][0], idn[2][1]]).map(lambda fin: fin[0].rstrip().split(' ')+ fin[1:])
    
    columns = ['tm_stmp', 'load_bal_typ', 'client_add', 'load_bal_add', 'req_prcs_tm', 'bknd_prcs_tm', 'resp_prcs_tm', 'lb_resp_cod', 'ins_resp_cod', 'clnt_rcvd_byt', 'snt_byt', 'url', 'usr_agnt', 'ssl_val', 'ssl_prot']
    rawDf = hc.createDataFrame(rddstg3, columns)
    
    #split_col = pyspark.sql.functions.split(rawDf['tm_stmp'], 'T')
    split_col = split(rawDf['tm_stmp'], 'T')
    #dtTmDf = rawDf.withColumn('date', split_col[0]).withColumn('time', split_col[1])
    dtTmDf = rawDf.withColumn('date', split_col[0].cast(DateType()))
    dtTmDf = dtTmDf.withColumn('time', split_col[1].cast(StringType()))
    
    #split_col = pyspark.sql.functions.split(dtTmDf['time'], ':')
    split_col = split(dtTmDf['time'], ':')
    #trnsfmdDf = dtTmDf.withColumn('hour', split_col[0]).withColumn('min', split_col[1])
    trnsfmdDf = dtTmDf.withColumn('hour', split_col[0].cast(IntegerType()))
    trnsfmdDf = trnsfmdDf.withColumn('min', split_col[1].cast(IntegerType()))
    
    unqIdDf = trnsfmdDf.withColumn('chck_valid', (trnsfmdDf.req_prcs_tm + trnsfmdDf.bknd_prcs_tm + trnsfmdDf.resp_prcs_tm))
    
    #urlPerUser = unqIdDf.select("uniq_id","url").map(lambda vals: vals).reduceByKey(lambda id, url: id + url).toDF(['user','urls'])
    
    def datetime_range(start, end, delta):
        current = start
        while current < end:
            yield current
            current += delta
    
    strt_tm = "00:00:00"
    end_tm = "23:59:59"
    d1 = datetime.datetime.strptime(strt_tm, '%H:%M:%S')#.%f')
    d2 = datetime.datetime.strptime(end_tm, '%H:%M:%S')
        
    intervals = [dt.strftime('%H:%M:%S') for dt in datetime_range(d1, d2, datetime.timedelta(minutes=10))]
    
    def add_interval(tm, ip):
        for j in range(1, len(intervals)):
            i = j-1
            substrTm = tm.split('.')
            substrIp = ip.split(':')[0]
            if substrTm[0] > intervals[i] and substrTm[0] <= intervals[j]:
                return intervals[i] + '-' + intervals[j] + '_' + substrIp
    
    appnd_interval = udf(add_interval)
    tmInterDf = unqIdDf.withColumn('interval',appnd_interval(unqIdDf['time'], unqIdDf['client_add']))
    #tmInterDf.select('interval','url').map(lambda vals: vals).reduceByKey(lambda id, url: id + url).toDF(['user','urls'])
    
    
    outputDf = tmInterDf.filter(tmInterDf.chck_valid != -3).select('interval','url').map(lambda vals: vals).reduceByKey(lambda id, url: id +','+ url).map(lambda key_val: (key_val[0], key_val[1], len(set(key_val[1].split(','))))).toDF(['user','urls_visited','unique_url_cnts'])
   
    print(outputDf.show(10)) 
    userSessionDf = tmInterDf.groupBy('client_add','url').sum('chck_valid')
    userSessionDf = userSessionDf.withColumnRenamed('sum(chck_valid)', 'tm_spent')
    
    #avgUsrTimeSpent = userSessionDf.select(avg('tm_spent')).head()[0] # not required
    print(userSessionDf.describe().show())  # avg time , max time
    maxTimeSpent = userSessionDf.select(max('tm_spent')).head()[0]
    print("Max time spent of any user" + str(maxTimeSpent))
    # as max time spent is 172.47423999999998, so users spending > 160 can be most engaged users
    mostEngdUsers = userSessionDf.filter(userSessionDf['tm_spent'] > 160)
    #print(mostEngdUsers.show())
    sc.stop()
