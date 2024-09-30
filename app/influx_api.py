from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from datetime import timedelta
from datetime import datetime as dt
import pymongo
from bson.son import SON
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from bson.json_util import dumps
from bson.json_util import loads
# from loguru import logger
import  pytz

import datetime
import csv


# import asyncio
myclient = pymongo.MongoClient("mongodb://localhost:27017/", username = "mongoadmin", password = "kruzi")
mydb = myclient["kruzi"]


token = "3cMNrZaasM1LIK_gUcNlfvjMERsLf4IOMD5P5EyoBwRLimXWa6-eNl0nm2gaZLxuZznMBPHWFsVMY6KU9DjvSQ=="
org = "kruzi"
server = "http://graphenelk.com:8086"
FluxClient = InfluxDBClient(
    server, token=token, org=org
)

# -------------------read_WeatherDataExtended-----------------
def read_WeatherDataExtended(site, srt, stp):
    bucket = "WeatherData"
    data   = {'max':{}, 'min':{}}
    field_integral  = "(Rain|PPFD)" #(field1|field2|field3)
    field_max       = "(Temp|Humi|Illu|Pres|VPD|WSpd|Rain|sMst|sTmp|PPFD)" 
    field_min       = "(Temp|Humi|Pres|VPD|sMst|sTmp|PPFD)" 
    try:
        
        query = ('from(bucket: "'+bucket+'")\
                |> range(start: '+srt+', stop: '+stp+')\
                |> filter(fn: (r) =>r[\"_measurement\"] ==\"'+site+'\")\
                |> mean()')         
        result = FluxClient.query_api().query(query, org)
        for table in result:
                for record in table.records:
                    data[record.get_field()] = round(record.get_value(),2) 
        
        query = ('from(bucket: "'+bucket+'")\
                |> range(start: '+srt+', stop: '+stp+')\
                |> filter(fn: (r) =>r[\"_measurement\"] ==\"'+site+'\")\
                |> filter(fn: (r) =>r[\"_field\"] =~ /^'+field_integral+'$/)\
                |> integral(unit:300s)')   
        result = FluxClient.query_api().query(query, org)
        for table in result:
            for record in table.records:
                data[record.get_field()] = round(record.get_value(),2)   
        
        query = ('from(bucket: "'+bucket+'")\
                |> range(start: '+srt+', stop: '+stp+')\
                |> filter(fn: (r) =>r[\"_measurement\"] ==\"'+site+'\")\
                |> filter(fn: (r) =>r[\"_field\"] =~ /^'+field_max+'$/)\
                |> max()')           
        result = FluxClient.query_api().query(query, org)
        for table in result:
            for record in table.records:
                data['max'][record.get_field()] = {'value':round(record.get_value(),2), 'time':record.get_time()} #.strftime("%Y-%m-%dT%H:%M:%SZ")}   
        
        query = ('from(bucket: "'+bucket+'")\
                |> range(start: '+srt+', stop: '+stp+')\
                |> filter(fn: (r) =>r[\"_measurement\"] ==\"'+site+'\")\
                |> filter(fn: (r) =>r[\"_field\"] =~ /^'+field_min+'$/)\
                |> min()')           
        result = FluxClient.query_api().query(query, org)   
        for table in result:
            for record in table.records:
                data['min'][record.get_field()] = {'value':round(record.get_value(),2), 'time':record.get_time()} #.strftime("%Y-%m-%dT%H:%M:%SZ")}   
    except:
        return {"message": "db error-", "ok": False, "data":data}
    else:    
        return {"message":"success", "ok": True, "data":data}        

# -------------------read_WeatherLTExtended-----------------
def read_WeatherHrExtended(site, srt, stp): 
    bucket = "WeatherHourly"
    data   = {'max':{}, 'min':{}}
    field_integral  = "(ETo)" #(field1|field2|field3)
    field_mean      = "(Tmp_dew)" 
    field_max       = "(Tmp_dew|ETo)" 
    field_min       = "(Tmp_dew|ETo)" 
    try:
        
        query = ('from(bucket: "'+bucket+'")\
                |> range(start: '+srt+', stop: '+stp+')\
                |> filter(fn: (r) =>r[\"_measurement\"] ==\"'+site+'\")\
                |> filter(fn: (r) =>r[\"_field\"] =~ /^'+field_mean+'$/)\
                |> mean()')         
        result = FluxClient.query_api().query(query, org)
        for table in result:
                for record in table.records:
                    data[record.get_field()] = round(record.get_value(),2)                 

        query = ('from(bucket: "'+bucket+'")\
                |> range(start: '+srt+', stop: '+stp+')\
                |> filter(fn: (r) =>r[\"_measurement\"] ==\"'+site+'\")\
                |> filter(fn: (r) =>r[\"_field\"] =~ /^'+field_integral+'$/)\
                |> integral(unit:3600s)')   
        result = FluxClient.query_api().query(query, org)
        for table in result:
            for record in table.records:
                data[record.get_field()] = round(record.get_value(),2)   

        query = ('from(bucket: "'+bucket+'")\
                |> range(start: '+srt+', stop: '+stp+')\
                |> filter(fn: (r) =>r[\"_measurement\"] ==\"'+site+'\")\
                |> filter(fn: (r) =>r[\"_field\"] =~ /^'+field_max+'$/)\
                |> max()')           
        result = FluxClient.query_api().query(query, org)
        for table in result:
            for record in table.records:
                data['max'][record.get_field()] = {'value':round(record.get_value(),2), 'time':record.get_time()} #.strftime("%Y-%m-%dT%H:%M:%SZ")}   
        
        query = ('from(bucket: "'+bucket+'")\
                |> range(start: '+srt+', stop: '+stp+')\
                |> filter(fn: (r) =>r[\"_measurement\"] ==\"'+site+'\")\
                |> filter(fn: (r) =>r[\"_field\"] =~ /^'+field_min+'$/)\
                |> min()')           
        result = FluxClient.query_api().query(query, org)
        for table in result:
            for record in table.records:
                data['min'][record.get_field()] = {'value':round(record.get_value(),2), 'time':record.get_time()} #.strftime("%Y-%m-%dT%H:%M:%SZ")}   
    except:
        return {"message": "db error-", "ok": False, "data":data}
    else:    
        return {"message":"success", "ok": True, "data":data}  

def read_WeatherDailyList(site, startDay, stopDay, timeZone):
    bucket = "WeatherData"
    dataList  = []
    field_average = "(Temp|Humi)"
    # convert the local date to UTC and covert to string for the influx query
    srt = startDay.split("-")
    stp = stopDay.split("-")
    srtL = datetime.datetime(int(srt[0]), int(srt[1]), int(srt[2]), 0, 0, 0)
    stpL = datetime.datetime(int(stp[0]), int(stp[1]), int(stp[2]), 0, 0, 0)
    srtL = pytz.timezone(timeZone).localize(srtL)
    stpL = pytz.timezone(timeZone).localize(stpL)
    srtZ = srtL.astimezone(pytz.timezone('UTC'))
    stpZ = stpL.astimezone(pytz.timezone('UTC'))
    T1 = srtZ
    try:
        while (T1<=stpZ):
            T2 = T1 +datetime.timedelta(hours=24)
            strT1 = T1.strftime("%Y-%m-%dT%H:%M:%S-00:00")
            strT2 = T2.strftime("%Y-%m-%dT%H:%M:%S-00:00")
            result = read_WeatherDataExtended(site, strT1, strT2)
            resultHr = read_WeatherHrExtended(site, strT1, strT2)
            #print (result)
            #print (resultHr)
            data = result['data']
            # combine the query results from weatherData and weatherHourly
            for key in ["ETo", "Tmp_dew"]:                          
                data[key] = resultHr['data'][key]
                data['max'][key] = resultHr['data']['max'][key]
                data['min'][key] = resultHr['data']['min'][key]
            # adjust time to local TZ and copy min and max values to outside
            for key in data['max']:
                data[key+"_max"] = data['max'][key]['value']
                T = data['max'][key]['time']
                dateZ = datetime.datetime(T.year, T.month, T.day, T.hour, T.minute, T.second)      # timezone ignorant datetime from the influx timestamp
                dateL = pytz.timezone('UTC').localize(dateZ).astimezone(pytz.timezone(timeZone))   # make it timezone aware (UTC) and convert to local timezone of the weatherstation  
                #print (key+"_max "+str(dateZ)+" "+str(dateL))
                data[key+"_max_at"] = dateL.strftime("%H:%M")
            del data['max']
            for key in data['min']:
                data[key+"_min"] = data['min'][key]['value']
                T = data['min'][key]['time']
                dateZ = datetime.datetime(T.year, T.month, T.day, T.hour, T.minute, T.second)      # timezone ignorant datetime from the influx timestamp
                dateL = pytz.timezone('UTC').localize(dateZ).astimezone(pytz.timezone(timeZone))   # make it timezone aware (UTC) and convert to local timezone of the weatherstation  
                #print (key+"_min "+str(dateZ)+" "+str(dateL))
                data[key+"_min_at"] = dateL.strftime("%H:%M")    
            del data['min'] 
            data['date'] = dateL.strftime("%Y-%m-%d") 

            #print (data)
            T1 = T1 +datetime.timedelta(hours=24)
            data['site']=site
            dataList.append(data)
        
    except:
        return {"message": "db error-", "ok": False, "data":dataList}
    else:    
        return {"message":"success", "ok": True, "data":dataList}   

def order_data(data):
    new_data = []
    
    for day in data:
        new = {}
        
        new = {
            "Date":day.get('date', "-"),
            "Ref Evapotranspiration - ETo":{
                "Daily Avg (mm/day)":day.get('ETo', "-"),
                "Highest (mm/hr)":day.get('ETo_max', "-"),
                "Highest at":day.get('ETo_max_at', "-"),
                "Lowest (mm/hr)":day.get('ETo_min', "-"),
                "Lowest at":day.get('ETo_min_at', "-")
            },
            "Vapor Pressure Deficit - VPD":{
                "Daily Avg (KPa)":day.get('VPD', "-"),
                "Highest (KPa)":day.get('VPD_max', "-"),
                "Highest at":day.get('VPD_max_at', "-"),
                "Lowest (KPa)":day.get('VPD_min', "-"),
                "Lowest at":day.get('VPD_min_at', "-")
            },
            "Dew Point":{
                "Daily Avg (°C)":day.get('Tmp_dew', "-"),
                "Highest (°C)":day.get('Tmp_dew_max', "-"),
                "Highest at":day.get('Tmp_dew_max_at', "-"),
                "Lowest (°C)":day.get('Tmp_dew_min', "-"),
                "Lowest at":day.get('Tmp_dew_min_at', "-")
            },
            "Day Light Integral - DLI":{
                "Daily Avg (mol/m²/day)":day.get('PPFD', "-"),
                "Highest (mol/m²/hr)":day.get('PPFD_max', "-"),
                "Highest at":day.get('PPFD_max_at', "-"),
                "Lowest (mol/m²/hr)":day.get('PPFD_min', "-"),
                "Lowest at":day.get('PPFD_min_at', "-")
            },
            "Rain":{
                "Daily Avg (mm/day)":day.get('Rain', "-"),
                "Highest (mm/5min)":day.get('Rain_max', "-"),
                "Highest at":day.get('Rain_max_at', "-")
            },
            "Temperature":{
                "Daily Avg (°C)":day.get('Temp', "-"),
                "Highest (°C)":day.get('Temp_max', "-"),
                "Highest at":day.get('Temp_max_at', "-"),
                "Lowest (°C)":day.get('Temp_min', "-"),
                "Lowest at":day.get('Temp_min_at', "-")
            },
            "Relative Humidity":{
                "Daily Avg (%)":day.get('Humi', "-"),
                "Highest (%)":day.get('Humi_max', "-"),
                "Highest at":day.get('Humi_max_at', "-"),
                "Lowest (%)":day.get('Humi_min', "-"),
                "Lowest at":day.get('Humi_min_at', "-")
            },
            "Sun Light Level":{
                "Daily Avg (Lux)":day.get('Illu', "-"),
                "Highest (Lux)":day.get('Illu_max', "-"),
                "Highest at":day.get('Illu_max_at', "-")
            },
            "Atmospheric Pressure":{
                "Daily Avg (mbar)":day.get('Pres', "-"),
                "Highest (mbar)":day.get('Pres_max', "-"),
                "Highest at":day.get('Pres_max_at', "-"),
                "Lowest (mbar)":day.get('Pres_min', "-"),
                "Lowest at":day.get('Pres_min_at', "-")
            },
            "Wind Direction":{
                "Daily Avg (°N)":day.get('WDir', "-")
            },
            "Wind Speed":{
                "Daily Avg (m/s)":day.get('WSpd', "-"),
                "Highest (m/s)":day.get('WSpd_max', "-"),
                "Highest at":day.get('WSpd_max_at', "-")
            },
            "Soil Moisture":{
                "Daily Avg (%)":day.get('sMst', "-"),
                "Highest (%)":day.get('sMst_max', "-"),
                "Highest at":day.get('sMst_max_at', "-"),
                "Lowest (%)":day.get('sMst_min', "-"),
                "Lowest at":day.get('sMst_min_at', "-")
            },
            "Soil Temperature":{
                "Daily Avg (°C)":day.get('sTmp', "-"),
                "Highest (°C)":day.get('sTmp_max', "-"),
                "Highest at":day.get('sTmp_max_at', "-"),
                "Lowest (°C)":day.get('sTmp_min', "-"),
                "Lowest at":day.get('sTmp_min_at', "-")
            }
        }
        new_data.append(new)
        
    return new_data
        

def backups(site):
    try:
        myquery = {"Site":site}
        mydoc = mydb["WeatherSt"].find_one(myquery)
    except:
        mydoc = "Error"
    return mydoc




async def collect_data(site,start_date,end_date):
    data = []
    
    start_date = dt.strptime(start_date, "%Y-%m-%d")
    end_date = dt.strptime(end_date, "%Y-%m-%d")
    
    current_date = start_date
    
    while current_date <= end_date:
        current_date_str = current_date.strftime("%Y-%m-%d")
        tmp_data = read_WeatherDailyList(site, current_date_str, current_date_str, "Asia/Colombo")
        if tmp_data['data'] == []:
            bckp = backups(site)
            bckp_wthrSt = bckp['Backup']
            i = 0
            while tmp_data['data'] == [] and i<len(bckp):
                tmp_data = read_WeatherDailyList(bckp_wthrSt[i], current_date_str, current_date_str, "Asia/Colombo")
                i += 1
        data.extend(tmp_data['data'])
        current_date += timedelta(days=1)
    
    
    # tmp_data=read_WeatherDailyList(site,start_date,end_date,"Asia/Colombo")
    # data=tmp_data['data']
    new_data = order_data(data)
    return new_data 

     # Write the data to the JSON file
    #  with open("2024_04_01.json", 'w') as json_file:  
    #     json.dump(data, json_file, indent=2)   
     
   
#out=read_WeatherDailyList("Rajanganaya","2023-12-23","2023-12-23","Asia/Colombo")
#print (out['data'])
#collect_data("2023-12-27","2024-01-04")
#collect_data("2024-01-17","2024-02-04")
#collect_data("2024-02-13","2024-02-21")

# async def main():
#     # Call the collect_data coroutine and await its result
#     a = await collect_data("Rajanganaya", "2024-09-01", "2024-09-02")
#     print(a)

# # Run the async main function
# asyncio.run(main())

#print(wather_daily("2023-12-17","2023-12-30")) 
#print(wather_daily_new("2023-12-15","2023-12-16"))
#export_to_csv_file("2023-12-16","2023-12-30")
     
