import ast
import sys
import pandas as pd
import sys
import os
import requests
import json
import copy
import datetime
import glob
import string
import kafka
import uuid
import re
import datetime
sys.path.append("..")



class post:

    def getEventConstants(self,path):
        LTLpath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-1]]) # to get E"://LTL_UIPAth

        eventMap = eval(open(LTLpath+"\\\\EventConstantsMap.txt").read())
        print(eventMap)
        eventMap =  {k.lower(): v for k, v in eventMap.items()}


        return eventMap, set(list(eventMap.keys()))

    def Event(self, eventMap, eventSet, event, logger):
        print("HI EVENT")
        #substring = event.split("at")[0].lower() + "at"
        if event.lower() in eventSet:
            return eventMap[event.lower()], event
        #elif substring in eventSet:
        #    return eventMap[substring.lower()], event
        else:
            return eventMap["UNMAPPED_Event_Code".lower()], event


    def EventPost(self,pronumber,LTLpath, path, config_default, logger,kafka_publisher):
        yrcdf = pd.read_csv(path+"\\\\"+pronumber+".csv")
        yrcdf = yrcdf.set_index("Column1")
        #print(yrcdf)
        #print(yrcdf.loc["Trailer #:"]['Data'])

        sys.path.append(LTLpath+"\\")

        import ShipmentEventBase as baseInfo
    
        postJson = copy.deepcopy(baseInfo.baseInfo.shipmentEventBase)
        logger.info(yrcdf)


        try:
            logger.info("inside first try for reading config")
            # print(count)
            postJson["uuid"] = str(uuid.uuid4())
            postJson["carrierCode"] = config_default["carrier_code"]
            # print(type(postJson["carrierCode"]))
            postJson["carrierName"] = config_default["carrier_name"]
            postJson["publisherCode"] = config_default["publisher_code"]
            postJson["customerName"] = config_default["customer_name"]
            postJson["resolvedEventSource"] = config_default["event_source"]



        except Exception as e:
            logger.info("There was an error in reading from config file. The error is : " + str(e))


        logger.info("reading from csv")
        postJson["eventType"] = "LTL"
        # postJson["eventTime"] = datetime.datetime.strptime(container_json["statusDate"], "%m/%d/%Y").strftime(
        #    "%Y-%m-%d")  # .strftime("%m-%d-%Y ")


        postJson["proNumber"] = yrcdf.loc["YRC Freight PRO #:"]['Data']
        
        postJson["trailerNumber"] = yrcdf.loc["Trailer #:"]['Data']

        eventMap, eventSet = self.getEventConstants(path)

        postJson["eventCode"], postJson["eventName"] = self.Event(eventMap, eventSet,yrcdf.loc["Status:"]['Data'], logger)
        logger.info(postJson["eventCode"])
        logger.info(postJson["eventName"])

        if(postJson["eventName"].lower().find("delivery")!=-1):
            postJson["eventName"]="Delivered"
        postJson["eventDate"]=datetime.datetime.strptime(yrcdf.loc["Status:"]['Data'].split("-")[0].split(":")[1], " %m/%d/%Y ").strftime("%Y-%m-%d")  # .strftime("%m-%d-%Y ")
             #split statusto get date
        

        postJson["signedBy"] = yrcdf.loc["Status:"]['Data'].split("-")[1].split(":")[1].strip()
        print(postJson["signedBy"])

        postJson["shipFrom"] = yrcdf.loc["Ship From:"]['Data']
        print(postJson["shipFrom"])
        postJson["shipTo"] = yrcdf.loc["Ship To:"]['Data']
        print(postJson["shipTo"])

        postJson["pickupDate"] = datetime.datetime.strptime(yrcdf.loc["Pickup Date:"]['Data'], "%m/%d/%Y").strftime(
            "%Y-%m-%d")
        postJson["deliveryDate"] = datetime.datetime.strptime(yrcdf.loc["Delivered Date:"]['Data'],
                                                              "%m/%d/%Y").strftime(
            "%Y-%m-%d")
        postJson["appointmentDate"] = datetime.datetime.strptime(yrcdf.loc["Appointment Date:"]['Data'],
                                                                 "%m/%d/%Y").strftime(
            "%Y-%m-%d")
        print(postJson["appointmentDate"])
        print(yrcdf.loc["Appointment Time:"]['Data'].split("-")[0])

        postJson["appointmentTimeStart"] = yrcdf.loc["Appointment Time:"]['Data'].split("-")[0]  # .strftime("%m-%d-%Y ")
        print(postJson["appointmentTimeStart"])
        postJson["appointmentTimeEnd"] = yrcdf.loc["Appointment Time:"]['Data'].split("-")[1]  # .strftime("%m-%d-%Y")
        print(postJson["appointmentTimeEnd"])
        print(postJson)
        

        with open(path + "\\\\Results\\\\" + yrcdf.loc["YRC Freight PRO #:"]['Data'] + "resultsStep" + ".json",
                  'w') as f:
            json.dump(postJson, f)
        kafka_publisher.publish(postJson)
       






def main(pronumberList,cwd,ENV_MODE):
    path = ""
    for x in cwd.split("\\"):
        path += x + "\\\\"
    sys.path.append(path + "\\")




    carrierName=cwd.split("\\")[-1]

    multiprocessPath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-3]]) #till blume multiprocess
    carrierPath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-4]])+"\\\\"+carrierName
    LTLpath = '\\\\'.join([str(elem) for elem in carrierPath.split("\\\\")[:-1]])
    #till yrc
    sys.path.append(LTLpath+"\\")
    from LTLCarrierLib import get_config, setup_logger

    pronumberList = list(set(pronumberList))
    config_default = get_config(carrierPath, str(ENV_MODE).strip())
    from KafkaPublisher import KafkaPublisher
    kafka_publisher = KafkaPublisher(server=ast.literal_eval(config_default['kafka_server']),
                                    topic=config_default['kafka_topic'])


    for pro in pronumberList:
        logger = setup_logger(
          pro + "_Logger",multiprocessPath+ "\\\\Logs\\\\" + pro + "_LOG.log"
        )




        logger.setLevel(config_default["logger_level"])
        logger.info("Pro Number formatting started")

        post_obj = post()
        # print(i)
        post_obj.EventPost(pro,LTLpath,carrierPath,config_default,logger,kafka_publisher)
    return multiprocessPath




if __name__ == "__main__":
    # testMain(sys.argv[1])
   # main()
   main(sys.argv[1], sys.argv[2],sys.argv[3])#, sys.argv[3])






