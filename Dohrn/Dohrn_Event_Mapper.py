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
        #print(eventMap)
        eventMap =  {k.lower(): v for k, v in eventMap.items()}


        return eventMap, set(list(eventMap.keys()))

    def Event(self, eventMap, eventSet, event, logger):

        #substring = event.split("at")[0].lower() + "at"
        if event.lower() in eventSet:
            return eventMap[event.lower()], event
        #elif substring in eventSet:
        #    return eventMap[substring.lower()], event
        else:
            return eventMap["UNMAPPED_Event_Code".lower()], event


    def EventPost(self,pronumber,LTLpath, path, config_default, logger,kafka_publisher):
        dohrndf = pd.read_csv(path+"\\\\"+pronumber+".csv")
        dohrndfservice = pd.read_csv(path+"\\\\"+pronumber + "additional.csv")
        dohrndfservice = dohrndfservice.rename(columns={"Total:": "Pieces", "Pieces:": "Pallets", "Pallets:": "Weight", "Weight:": "Service Type"})
        dohrndfstatus= pd.read_csv(path+"\\\\"+pronumber + "status.csv")
        dohrndfstatus=dohrndfstatus.set_index("Title")



        sys.path.append(LTLpath+"\\")

        import ShipmentEventBase as baseInfo
    
        postJson = copy.deepcopy(baseInfo.baseInfo.shipmentEventBase)
        #logger.info(dohrndf)




        for i in range(0,len(dohrndf)):
            try:
                logger.info("inside first try for reading config")
            # print(count)
                postJson["uuid"] = str(uuid.uuid4())
             #   print(postJson["uuid"])
                postJson["carrierCode"] = config_default["carrier_code"]
            # print(type(postJson["carrierCode"]))
                postJson["carrierName"] = config_default["carrier_name"]
             #   print(postJson["carrierName"])
                postJson["publisherCode"] = config_default["publisher_code"]
             #   print(postJson["publisherCode"])
                postJson["customerName"] = config_default["customer_name"]
             #   print(postJson["customerName"])
                postJson["resolvedEventSource"] = config_default["resolvedEventSource"]
              #  print(postJson["resolvedEventSource"]+"1")



            except Exception as e:
                logger.info("There was an error in reading from config file. The error is : " + str(e))


            logger.info("reading from csv")
            postJson["eventType"] = "LTL"
        # postJson["eventTime"] = datetime.datetime.strptime(container_json["statusDate"], "%m/%d/%Y").strftime(
        #    "%Y-%m-%d")  # .strftime("%m-%d-%Y ")
            try:
                

                postJson["proNumber"] = pronumber
                postJson["quantity"] = str(dohrndfservice.iloc[0]['Pieces'])
             #   print(type(postJson["quantity"]))
                postJson["weight"] = str(dohrndfservice.iloc[0]['Weight'])
             #   print(postJson["weight"])
                postJson["trailerNumber"] = dohrndfstatus.loc["Trailer #:"]['Data']
            #    print(type(postJson["trailerNumber"]))
                postJson["shipFrom"] = dohrndfstatus.loc["Ship From:"]['Data']
             #   print(postJson["shipFrom"])
                postJson["shipTo"] = dohrndfstatus.loc["Deliver To:"]['Data']
             #   print(postJson["shipTo"])

                postJson["signedBy"] = dohrndfstatus.loc["Signature:"]["Data"]
             #   print(postJson["signedBy"])
                postJson["pickupDate"] = pd.datetime.datetime.strptime(dohrndfstatus.loc["Ship Date:"]['Data'],"%m/%d/%Y").strftime("%Y-%m-%d")  # here ship date
             #   print(postJson["pickupDate"])
                postJson["deliveryDate"] = pd.datetime.datetime.strptime(dohrndfstatus.loc['Delivered Date:']['Data'],"%m/%d/%Y").strftime("%Y-%m-%d")
              #  print(postJson["deliveryDate"])

                if (dohrndfstatus.loc["Status:"]['Data'].lower().find("delivered") != -1):
                    postJson["currentStatus"] = "Delivered"
                else:
                    postJson["currentStatus"]=dohrndfstatus.loc["Status:"]['Data']

        #postJson["trailerNumber"] = yrcdf.loc["Trailer #:"]['Data']

                eventMap, eventSet = self.getEventConstants(path)

                postJson["eventCode"], postJson["eventName"] = self.Event(eventMap, eventSet,dohrndf.iloc[i]['Shipment Status'], logger)
              #  print(postJson["eventCode"])
                logger.info(postJson["eventCode"])
                logger.info(postJson["eventName"])


             #   print(postJson["eventName"])
                postJson["eventDate"]=datetime.datetime.strptime(dohrndf.loc[i]['Date'],"%m/%d/%Y").strftime("%m-%d-%Y")
             #   print(postJson["eventDate"])
             #   print(postJson)
             #split statusto get date


                #postJson["signedBy"] = yrcdf.loc["Status:"]['Data'].split("-")[1].split(":")[1].strip()
                #print(postJson["signedBy"])
                with open(path + "\\\\Results\\\\" + pronumber + "resultsStep" + str(i) + ".json",
                      'w') as f:
                    json.dump(postJson, f)
            except Exception as e:
                logger.info("There was an error in reading from csv. The error is : " + str(e))
            kafka_publisher.publish(postJson)


'''
                if(i>0):
                    postJson["shipFrom"] = dohrndf.iloc[i-1]["Location"]
                else:
                    postJson["shipFrom"]=""
                print(postJson["shipFrom"])
                postJson["shipTo"] = dohrndf.loc[i]['Location']
                print(postJson["shipTo"])
'''







        



       






def main(pronumberList,cwd,ENV_MODE):
    path = ""
    for x in cwd.split("\\"):
        path += x + "\\\\"
    sys.path.append(path + "\\")




    carrierName=cwd.split("\\")[-1]

    multiprocessPath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-3]]) #till blume multiprocess
    carrierPath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-4]])+"\\\\"+carrierName
    LTLpath = '\\\\'.join([str(elem) for elem in carrierPath.split("\\\\")[:-1]])
    #till dohrn
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
    return carrierPath




if __name__ == "__main__":

   main(sys.argv[1], sys.argv[2],sys.argv[3])








