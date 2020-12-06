#!usr/bin/python3
import findspark
findspark.init()
# Standard Imports
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType,FloatType
import json
import os
import socket
import sys
import pyspark.sql.functions as f
sc = SparkContext(appName="FPLproj",master="local[4]").getOrCreate()
ssc = SparkSession(sc)
sql = SQLContext(sc)
from pyspark.sql.functions import udf, array
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
    StructField("rating", IntegerType(), False), \
    
  ])
#Teams Schema
tschema = StructType([\
    StructField("name",StringType(),True), \
    StructField("Id",IntegerType(),True), \
 ])
 # Load the Players and Teams data from CSV file
pRDD = ssc.read.csv(r"players.csv", schema=pschema, header=True)
tRDD = ssc.read.csv(r"teams.csv", schema=tschema, header=True)
pRDD=pRDD.na.fill(0)
Ids= pRDD.select("Id").rdd.flatMap(lambda x: x).collect()
columns=['Id','np', 'kp','anp', 'akp','pass_acc','dwc', 'dnc','dlc','total_duels', 'duel_eff', 'eff_k', 'penalty', 'total_kicks','fkick_eff', 'total_shots', 'shots_on_target',  'sot_goal','shots_eff', 'mfoul', 'own_goals','contrib','player_perf','rating','chemistry']
z=[(i,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.5,0.5) for i in Ids]
mRDD=ssc.createDataFrame(z,columns)
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
player_contrib = udf(lambda arr: sum(arr)/4, FloatType())
pair_cols=["p1","p2","chemistry"]
pairs=[(i,j,0.5) for i in Ids for j in Ids  if i!=j ]
chem_pairs=ssc.createDataFrame(pairs,pair_cols)   
def chemistry(i,j,t):
    previ=pRDD.filter(pRDD.Id==i).collect()[0][19]
    curri=mRDD.filter(mRDD.Id==i).collect()[0][24]
    prevj=pRDD.filter(pRDD.Id==j).collect()[0][19]
    currj=mRDD.filter(mRDD.Id==j).collect()[0][24]
    rate1=previ-curri
    rate2=prevj-currj
    chem=abs((rate1+rate2)/2)
    if(t=='s'):
        same_chemistry(i,j,rate1,rate2,chem)
    if(t=='o'):
        opp_chemistry(i,j,rate1,rate2,chem)
def opp_chemistry(i,j,rate1,rate2,chem):
    global chem_pairs
    c=chem_pairs.filter((F.col('p1')==i) & (F.col('p2')==j)).collect()[0]
    if((rate1<0 and rate2<0) or (rate1>0 and rate2>0)):
        chem_pairs=chem_pairs.withColumn("chemistry",F.when((F.col("p1")==i) & (F.col("p2")==j),c[2]-chem).otherwise(chem_pairs["chemistry"]))
    if((rate1<0 and rate2>0) or (rate1>0 and rate2<0)):
        chem_pairs=chem_pairs.withColumn("chemistry",F.when((F.col("p1")==i) & (F.col("p2")==j),c[2]+chem).otherwise(chem_pairs["chemistry"]))
def same_chemistry(i,j,rate1,rate2,chem):
    global chem_pairs
    c=chem_pairs.filter((F.col('p1')==i) & (F.col('p2')==j)).collect()[0]
    if((rate1<0 and rate2<0) or (rate1>0 and rate2>0)):
        chem_pairs=chem_pairs.withColumn("chemistry",F.when((F.col("p1")==i) & (F.col("p2")==j),c[2]+chem).otherwise(chem_pairs["chemistry"]))
    if((rate1<0 and rate2>0) or (rate1>0 and rate2<0)):
        chem_pairs=chem_pairs.withColumn("chemistry",F.when((F.col("p1")==i) & (F.col("p2")==j),c[2]-chem).otherwise(chem_pairs["chemistry"]))
def profmetrics(batch):
    global pRDD
    global tRDD
    global mRDD
    #global metrics
    data=[json.loads(rdd) for rdd in batch.collect()]
    match_data=list()
    for i in data:
        #print(i)
        if 'eventId' in i:
            ids=[j[k] for j in i['tags'] for k in j ]
            #print(ids)
            #print(pRDD.filter(i['playerId']==pRDD.Id).collect())
            #print(ids)
            pid=i['playerId']
            df=pRDD.filter(pid==pRDD.Id).collect()
            #print(df)
            if(len(df)>0):
                df=df[0]
                matchdf=mRDD.filter(pid==mRDD.Id).collect()[0]
                if(i['eventId']==8):#pass
                    np=matchdf[1]
                    kp=matchdf[2]
                    nap=matchdf[3]
                    akp=matchdf[4]
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
                    mRDD=mRDD.withColumn("pass_acc",F.when(F.col("Id")==pid,p_acc).otherwise(mRDD["pass_acc"]))
                    mRDD=mRDD.withColumn("akp",F.when(F.col("Id")==pid,akp).otherwise(mRDD["akp"]))
                    mRDD=mRDD.withColumn("anp",F.when(F.col("Id")==pid,nap).otherwise(mRDD["anp"]))
                    mRDD=mRDD.withColumn("np",F.when(F.col("Id")==pid,np).otherwise(mRDD["np"]))
                    mRDD=mRDD.withColumn("kp",F.when(F.col("Id")==pid,kp).otherwise(mRDD["kp"]))
                    pRDD=pRDD.withColumn("pass_acc",F.when(F.col("Id")==pid,p_acc).otherwise(pRDD["pass_acc"]))
                    pRDD=pRDD.withColumn("num_acc_key_pass",F.when(F.col("Id")==pid,num_acc_key_pass).otherwise(pRDD["num_acc_key_pass"]))
                    pRDD=pRDD.withColumn("num_normal_pass",F.when(F.col("Id")==pid,num_normal_pass).otherwise(pRDD["num_normal_pass"]))
                    pRDD=pRDD.withColumn("num_key_pass",F.when(F.col("Id")==pid,num_key_pass).otherwise(pRDD["num_key_pass"]))
                    print('pa',pRDD.filter(i['playerId']==pRDD.Id).collect()[0][12])
                if(i['eventId']==1):#duel
                    wc=matchdf[6]
                    nc=matchdf[7]
                    lc=matchdf[8]
                    total_duels=matchdf[9]
                    if(701 in ids):
                        wc=wc+1
                    elif(702 in ids):
                        nc=nc+1
                    elif(703 in ids):
                        lc=lc+1
                    total_duels=total_duels+wc+nc+lc
                    deff=duel_eff(wc,nc,total_duels)
                    mRDD=mRDD.withColumn("dwc",F.when(F.col("Id")==pid,wc).otherwise(mRDD["dwc"]))
                    mRDD=mRDD.withColumn("dnc",F.when(F.col("Id")==pid,nc).otherwise(mRDD["dnc"]))
                    mRDD=mRDD.withColumn("dlc",F.when(F.col("Id")==pid,lc).otherwise(mRDD["dlc"]))
                    mRDD=mRDD.withColumn("total_duels",F.when(F.col("Id")==pid,total_duels).otherwise(mRDD["total_duels"]))
                    mRDD=mRDD.withColumn("duel_eff",F.when(F.col("Id")==pid,deff).otherwise(mRDD["duel_eff"]))
                    pRDD=pRDD.withColumn("duel_eff",F.when(F.col("Id")==pid,deff).otherwise(pRDD["duel_eff"]))
                    print('da',pRDD.filter(i['playerId']==pRDD.Id).collect()[0][17])
                if(i['eventId']==3):#free kick
                    keff=matchdf[11]
                    total_kicks=matchdf[13]
                    penalty=matchdf[12]
                    fkick_eff=matchdf[14]
                    goal=0
                    if(1801 in ids):
                        keff=keff+1
                    if(i['subEventId']==35):
                        if(101 in ids):
                            goals=goals+1
                        penalty=penalty+1
                    total_kicks=total_kicks+1
                    fkick_eff=free_kick_eff(keff,penalty,total_kicks)
                    mRDD=mRDD.withColumn("eff_k",F.when(F.col("Id")==pid,keff).otherwise(mRDD["eff_k"]))
                    mRDD=mRDD.withColumn("penalty",F.when(F.col("Id")==pid,penalty).otherwise(mRDD["penalty"]))
                    mRDD=mRDD.withColumn("total_kicks",F.when(F.col("Id")==pid,total_kicks).otherwise(mRDD["total_kicks"]))
                    mRDD=mRDD.withColumn("fkick_eff",F.when(F.col("Id")==pid,fkick_eff).otherwise(mRDD["fkick_eff"]))
                    print(fkick_eff)
                if(i['eventId']==10):#shots
                    total_shots=matchdf[15]
                    shots_on_target=matchdf[16]
                    sot_goal=matchdf[17]
                    total_shots=total_shots+1
                    psot=df[15]
                    if(1801 in ids):
                        if(101 in ids):
                            sot_goal=sot_goal+1
                        else:
                            shots_on_target=shots_on_target+1
                    shotseff=shots_eff(sot_goal,shots_on_target,total_shots)
                    mRDD=mRDD.withColumn("total_shots",F.when(F.col("Id")==pid,total_shots).otherwise(mRDD["total_shots"]))
                    mRDD=mRDD.withColumn("shots_on_target",F.when(F.col("Id")==pid,shots_on_target).otherwise(mRDD["shots_on_target"]))
                    mRDD=mRDD.withColumn("sot_goal",F.when(F.col("Id")==pid,sot_goal).otherwise(mRDD["sot_goal"]))
                    mRDD=mRDD.withColumn("shotseff",F.when(F.col("Id")==pid,shotseff).otherwise(mRDD["shotseff"]))
                    psot=psot+shots_on_target
                    pRDD=pRDD.withColumn("shots_on_target",F.when(F.col("Id")==i['playerId'],psot).otherwise(pRDD["shots_on_target"]))
                    #print('shots_on_target',pRDD.filter(i['playerId']==pRDD.Id).collect()[0][18])
                    #print(shotseff)
                if(i['eventId']==2):
                    fouls=df[9]
                    mfoul=matchdf[18]
                    mfoul=mfoul+1
                    fouls=fouls+mfoul
                    mRDD=mRDD.withColumn("mfoul",F.when(F.col("Id")==pid,mfoul).otherwise(mRDD["mfoul"]))
                    pRDD=pRDD.withColumn("fouls",F.when(F.col("Id")==i['playerId'],fouls).otherwise(pRDD["fouls"]))
                if(i['eventId']==102):
                    owngoals=df[11]
                    ogoals=matchdf[19]
                    owngoals=owngoals+1
                    ogoals=ogoals+1
                    mRDD=mRDD.withColumn("owngoals",F.when(F.col("Id")==pid,ogoals).otherwise(mRDD["owngoals"]))
                    pRDD=pRDD.withColumn("owngoals",F.when(F.col("Id")==i['playerId'],fouls).otherwise(pRDD["owngoals"]))
        
        else:
            if len(match_data):
                subs=list()
                for i in match_data['teamsData']:
                    for j in match_data['teamsData'][i]['formation']['substitutions'] :
                        sub_in=j['playerIn']
                        sub_out=j['playerOut']
                        minute=j['minute']
                        subs.append(sub_in)
                        subs.append(sub_out)
                        mRDD=mRDD.withColumn("contrib",F.when(F.col("Id")==sub_in,player_contrib(array("pass_acc","duel_eff","fkick_eff","shots_on_target"))*(minute/90)).otherwise(mRDD["contrib"]))
                        mRDD=mRDD.withColumn("contrib",F.when(F.col("Id")==sub_out,player_contrib(array("pass_acc","duel_eff","fkick_eff","shots_on_target"))*(minute/90)).otherwise(mRDD["contrib"]))
                mRDD=mRDD.withColumn("contrib",F.when(~F.col("Id").isin(subs),player_contrib(array("pass_acc","duel_eff","fkick_eff","shots_on_target"))*(1.05)).otherwise(mRDD["contrib"]))
                #print(mRDD.select('contrib').collect()) 
                performance = udf(lambda arr:arr[0]-(0.005*arr[1]+0.05*arr[2]),FloatType())
                player_rating=udf(lambda arr:sum(arr)/2,FloatType())
                mRDD=mRDD.withColumn("player_perf",performance(array("contrib","mfoul","own_goals")))
                mRDD=mRDD.withColumn("rating",player_rating(array("player_perf","rating")))
                teamid=dict()
                for k in match_data["teamsData"]:
                    played=set()
                    for l in match_data["teamsData"][k]['formation']['lineup']:
                        played.add(l['playerId'])
                    for m in match_data["teamsData"][k]['formation']['substitutions']:
                        played.add(m['playerIn'])
                        played.add(m['playerOut'])
                    teamid[k]=played
                tids=[i for i in teamid.keys()]
                w=[ chemistry(i,j,'s') for i in teamid[tids[0]] for j in teamid[tids[0]]  if i!=j ]
                v=[ chemistry(i,j,'o') for i in teamid[tids[0]] for j in teamid[tids[1]]]
                pRDD=pRDD.withColumn("rating",player_rating(array("player_perf","rating")))
            match_data=i
strc = StreamingContext(sc, 5)
lines = strc.socketTextStream('localhost', 6100)
#lines.pprint()
lines.foreachRDD(profmetrics)
strc.start()
strc.awaitTermination()  
strc.stop(stopSparkContext=False, stopGraceFully=True)

 
