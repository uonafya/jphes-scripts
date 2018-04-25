#Script for JPHES Attribution
import psycopg2
import pandas as pd
import numpy as np

#Defaults
nutrition_dataelementgroupid=2046015
mch_dataelementgroupid=2046012
monthly_periodtypeid=3
nutrition_filter='Nutrition'
mch_filter='MCH'

count_executions=[]

# DB Connection
conn_str = "host={} port={} dbname={} user={} password={}".format("localhost","5432", "db_name", "user", "password")
conn=None
try:
    conn = psycopg2.connect(conn_str)
   
    
    ipsl_members=pd.read_sql("select * from jphes_ipsl", con=conn)
    
    #select data elements for a specific Program - align the filter for programs and dataelementgroup
    dataelements=pd.read_sql("select dataelementid from dataelementgroupmembers \
                             where dataelementgroupid={}".format(mch_dataelementgroupid), con=conn)
    
    #filter orgunits for a specific program
    ipsl_orgunits=ipsl_members[ipsl_members.programs.str.contains(mch_filter,case=False)]
    
    # Filter by Partner(if need be)
    #ipsl_orgunits=ipsl_orgunits[ipsl_orgunits.mechanism_name.str.contains("Afya Uzazi", case=False)]
    #filter by quarter
    ipsl_orgunits=ipsl_orgunits[ipsl_orgunits.period.str.contains("2018Q2", case=False)]

    #period
    periods=pd.read_sql("select periodid,startdate, enddate from period \
                        where startdate>='2018-1-1' and enddate<='2018-3-31' \
                        and periodtypeid={}".format(monthly_periodtypeid), con=conn)
    
    
    cursor = conn.cursor()

    print("Started attribution process...")
    for dataelementid in dataelements.dataelementid:
        for period in periods.periodid:
            for index, orgunit in ipsl_orgunits.iterrows():
                    
                cursor.execute("UPDATE datavalue set attributeoptioncomboid = %s WHERE sourceid = %s "
                "AND dataelementid = %s AND periodid = %s" % (orgunit['mechanism_attributionid'], 
                                              orgunit['orgunitid'], dataelementid, period))
                
                
                count_executions.append(cursor.rowcount)
                
                conn.commit()
    
    cursor.close()
    
    print("---------------------------------------")
    print("Successful Updates-{}".format(np.sum(count_executions)))
    print("Total-{}".format(np.size(count_executions)))
    print("---------------------------------------")

    print("Attribution Complete")
            
                               
except Exception as ex:
                               
    print("Error Updating-{}".format(ex))
                               
finally:
        if conn is not None:
            conn.close()
