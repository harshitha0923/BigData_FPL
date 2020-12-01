#!usr/bin/python3
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType
import json
import os
import socket
import sys
sc = SparkContext(appName="FPLproj",master="local[4]").getOrCreate()
ssc = SparkSession(sc)
sql = SQLContext(sc)
pschema = StructType([ \
    StructField("name",StringType(),False), \
    StructField("birthArea",StringType(),False), \
    StructField("birthDate",TimestampType(),False), \
    StructField("foot", StringType(), False), \
    StructField("role", StringType(), False), \
    StructField("height", IntegerType(),False), \
    StructField("passportArea", StringType(), False), \
    StructField("weight", IntegerType(),False), \
    StructField("Id", IntegerType(),False), \
    StructField("fouls", IntegerType(),False), \
    StructField("goals", IntegerType(), False), \
    StructField("owngoals", IntegerType(),False), \
    StructField("pass_acc", IntegerType(),False), \
    StructField("num_acc_normal_pass", IntegerType(),False), \
    StructField("num_acc_key_pass", IntegerType(), False), \
    StructField("num_normal_pass", IntegerType(), False), \
    StructField("num_key_pass", IntegerType(), False), \
    StructField("duel_eff", IntegerType(), False), \
    StructField("shots_on_target", IntegerType(), False), \
    
  ])
#Teams Schema
tschema = StructType([\
    StructField("name",StringType(),True), \
    StructField("Id",IntegerType(),True), \
 ])
pRDD = ssc.read.csv(r"C:\Users\Sarvesh\Downloads\Data\players.csv", schema=pschema, header=True)
tRDD = ssc.read.csv(r"C:\Users\Sarvesh\Downloads\Data\teams.csv", schema=tschema, header=True)
sql.registerDataFrameAsTable(pRDD, "pRDD")
sql.registerDataFrameAsTable(tRDD, "pRDD")
pRDD=pRDD.na.fill(0)
Ids= pRDD.select("Id").rdd.flatMap(lambda x: x).collect()
m=dict.fromkeys(Ids) 
#metrics={i: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0] for i in m }
def pass_acc(num_acc_key_pass,num_normal_pass,num_key_pass,num_acc_normal_pass):
    pacc=(num_acc_normal_pass+(num_acc_key_pass*2))/(num_normal_pass+(num_key_pass*2))
    return pacc
def duel_eff(wc,nc,total_duels):
    dueleff=(wc+(nc*0.5))/total_duels
    return dueleff 
def free_kick_eff(keff,penalty,total_kicks):
    fkeff=(keff+penalty)/total_kicks
    return fkeff
def shots_eff(sot_goal,shots_on_target,total_shots):
    seff=(sot_goal+(shots_on_target*0.5))/total_shots
    return seff
def calc_player_contr(pass_accuracy, duel_effectiveness,free_kick_effectiveness, shots_on_trgt):
    x = (pass_accuracy + duel_effectiveness + free_kick_effectiveness + shots_on_trgt) / 4
    return x
def profmetrics(batch):
    global pRDD
    global tRDD
    global sql
    data=[json.loads(rdd) for rdd in batch.collect()]
    metrics={i: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0] for i in m }
    for i in data:
        #print(i)
        if 'eventId' in i:
            ids=[j[k] for j in i['tags'] for k in j ]
            #print(ids)
            #print(pRDD.filter(i['playerId']==pRDD.Id).collect())
            #print(ids)
            df=pRDD.filter(i['playerId']==pRDD.Id).collect()
            #print(df)
            if(len(df)>0):
                df=df[0]
                if(i['eventId']==8):#pass
                    nap=metrics[i['playerId']][0]
                    akp=metrics[i['playerId']][1]
                    kp=metrics[i['playerId']][2]
                    np=metrics[i['playerId']][3]
                    #print(nap,akp,kp,np)
                    num_acc_key_pass=df[14]
                    num_normal_pass=df[15]
                    num_key_pass=df[16]
                    num_acc_normal_pass=df[13]
                    if(1801 in ids):
                        #print(1801 in ids)
                        if(302 in ids):
                            #num_acc_key_pass=num_acc_key_pass+1
                            akp=akp+1
                            kp=kp+1
                        else:
                            #num_acc_normal_pass=num_acc_normal_pass+1
                            nap=nap+1
                            np=np+1
                    elif(1802 in ids):
                        #num_normal_pass=num_normal_pass+1
                        np=np+1
                    elif(302 in ids):
                        #num_key_pass=num_key_pass+1
                        kp=kp+1
                    #print(nap,akp,kp,np)
                    num_acc_key_pass= num_acc_key_pass+akp
                    num_normal_pass=num_normal_pass+np
                    num_key_pass=num_key_pass+kp
                    num_acc_normal_pass=num_acc_normal_pass+nap
                    #print(num_acc_key_pass,num_normal_pass,num_key_pass,num_acc_normal_pass)
                    p_acc=pass_acc(num_acc_key_pass,num_normal_pass,num_key_pass,num_acc_normal_pass)
                    metrics[i['playerId']][0]=nap
                    metrics[i['playerId']][1]=akp
                    metrics[i['playerId']][2]=kp
                    metrics[i['playerId']][3]=np
                    metrics[i['playerId']][4]=p_acc
                    pRDD=pRDD.withColumn("pass_acc",F.when(F.col("Id")==i['playerId'],p_acc).otherwise(pRDD["pass_acc"]))
                    pRDD=pRDD.withColumn("num_acc_key_pass",F.when(F.col("Id")==i['playerId'],num_acc_key_pass).otherwise(pRDD["num_acc_key_pass"]))
                    pRDD=pRDD.withColumn("num_normal_pass",F.when(F.col("Id")==i['playerId'],num_normal_pass).otherwise(pRDD["num_normal_pass"]))
                    pRDD=pRDD.withColumn("num_key_pass",F.when(F.col("Id")==i['playerId'],num_key_pass).otherwise(pRDD["num_key_pass"]))
                    print('pa',pRDD.filter(i['playerId']==pRDD.Id).collect()[0][12])
                if(i['eventId']==1):#duel
                    wc=metrics[i['playerId']][5]
                    nc=metrics[i['playerId']][6]
                    lc=metrics[i['playerId']][7]
                    total_duels=metrics[i['playerId']][8]
                    if(701 in ids):
                        wc=wc+1
                    elif(702 in ids):
                        nc=nc+1
                    elif(703 in ids):
                        lc=lc+1
                    total_duels=total_duels+wc+nc+lc
                    deff=duel_eff(wc,nc,total_duels)
                    metrics[i['playerId']][5]= wc
                    metrics[i['playerId']][6]= nc
                    metrics[i['playerId']][7]= lc
                    metrics[i['playerId']][8]=total_duels
                    metrics[i['playerId']][9]=deff
                    pRDD=pRDD.withColumn("duel_eff",F.when(F.col("Id")==i['playerId'],deff).otherwise(pRDD["duel_eff"]))
                    print('de',pRDD.filter(i['playerId']==pRDD.Id).collect()[0][17])
                if(i['eventId']==3):#free kick
                    keff=metrics[i['playerId']][10]
                    total_kicks=metrics[i['playerId']][11]
                    penalty=metrics[i['playerId']][12]
                    fkick_eff=metrics[i['playerId']][13]
                    goal=0
                    if(1801 in ids):
                        keff=keff+1
                    if(i['subEventId']==35):
                        if(101 in ids):
                            goals=goals+1
                        penalty=penalty+1
                    total_kicks=total_kicks+1
                    fkick_eff=free_kick_eff(keff,penalty,total_kicks)
                    metrics[i['playerId']][10]=keff
                    metrics[i['playerId']][11]=total_kicks
                    metrics[i['playerId']][12]=penalty
                    metrics[i['playerId']][13]=fkick_eff
                    print(fkick_eff)
                if(i['eventId']==10):#shots
                    total_shots=metrics[i['playerId']][14]
                    shots_on_target=metrics[i['playerId']][15]
                    sot_goal=metrics[i['playerId']][16]
                    total_shots=total_shots+1
                    psot=df[15]
                    if(1801 in ids):
                        if(101 in ids):
                            sot_goal=sot_goal+1
                        else:
                            shots_on_target=shots_on_target+1
                    shotseff=shots_eff(sot_goal,shots_on_target,total_shots)
                    metrics[i['playerId']][14]=total_shots
                    metrics[i['playerId']][15]=shots_on_target
                    metrics[i['playerId']][16]=sot_goal
                    metrics[i['playerId']][17]=shotseff
                    psot=psot+shots_on_target
                    pRDD=pRDD.withColumn("shots_on_target",F.when(F.col("Id")==i['playerId'],psot).otherwise(pRDD["shots_on_target"]))
                    #print('shots_on_target',pRDD.filter(i['playerId']==pRDD.Id).collect()[0][18])
                    #print(shotseff)
                if(i['eventId']==2):
                    fouls=df[9]
                    mfoul=metrics[i['playerId']][18]
                    mfoul=mfoul+1
                    fouls=fouls+mfoul
                    metrics[i['playerId']][18]=mfoul
                    pRDD=pRDD.withColumn("fouls",F.when(F.col("Id")==i['playerId'],fouls).otherwise(pRDD["fouls"]))
                if(i['eventId']==102):
                    owngoals=df[11]
                    ogoals=metrics[i['playerId']][19]
                    owngoals=owngoals+1
                    ogoals=ogoals+1
                    metrics[i['playerId']][19]=ogoals
                    pRDD=pRDD.withColumn("owngoals",F.when(F.col("Id")==i['playerId'],fouls).otherwise(pRDD["owngoals"]))
        
        else:
            print("***")
strc = StreamingContext(sc, 5)
lines = strc.socketTextStream('localhost', 6100)
#lines.pprint()
lines.foreachRDD(profmetrics)
strc.start()
strc.awaitTermination()  
strc.stop(stopSparkContext=False, stopGraceFully=True)                       

                        
                            
                        
                        
                    
                    
                    
                

                    
                    
                    
                    
                    
            