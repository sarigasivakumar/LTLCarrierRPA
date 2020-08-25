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

    def getEventConstants(self, path):
        LTLpath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-1]])  # to get E"://LTL_UIPAth

        eventMap = eval(open(LTLpath + "\\\\EventConstantsMap.txt").read())
        # print(eventMap)
        eventMap = {k.lower(): v for k, v in eventMap.items()}

        return eventMap, set(list(eventMap.keys()))

    def Event(self, eventMap, eventSet, event, logger):

        # substring = event.split("at")[0].lower() + "at"
        if event.lower() in eventSet:
            return eventMap[event.lower()], event
        # elif substring in eventSet:
        #    return eventMap[substring.lower()], event
        else:
            return eventMap["UNMAPPED_Event_Code".lower()], event

    def EventPost(self, pronumber, LTLpath, path, config_default, logger, kafka_publisher):
        MWdf = pd.read_csv(path + "\\\\" + pronumber + ".csv")

        MWdfstatus = pd.read_csv(path + "\\\\" + pronumber + "status.csv")
        #MWdfstatus["Column-0"].strip()

        MWdfstatus = MWdfstatus.set_index("Column-0")
        #print(MWdfstatus.loc["Origin:"]["Column-1"])

        sys.path.append(LTLpath + "\\")

        import ShipmentEventBase as baseInfo

        postJson = copy.deepcopy(baseInfo.baseInfo.shipmentEventBase)
        # logger.info(dohrndf)
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
        MWdf.fillna('',inplace=True)


        postJson["currentStatus"]=MWdfstatus.loc["Current Status:"]["Column-1"].split("\n")[0]

        for i in range(0, len(MWdf)):
            try:
                if(MWdf.loc[i]["Date"]==""):
                    MWdf.loc[i]["Date"]=MWdf.loc[i-1]["Date"]
                    #MWdf.loc[i]["Date"]=MWdf.loc[i-1]["Date"]
            except Exception as e:
                logger.info("There was an error in reading from csv. The error is : " + str(e))
                #postJson["eventDate"]=
        try:
            for i in range(0,len(MWdf)):
                postJson["eventDate"]=datetime.datetime.strptime(MWdf.iloc[i]["Date"],"%m/%d/%Y").strftime("%m-%d-%Y")

                if(MWdf.iloc[i]["Time"].find("AM")!=-1):
                    postJson["eventTime"]=MWdf.iloc[i]["Time"]
                else:
                    timehrmin=MWdf.iloc[i]["Time"].split(" ")[0]
                    postJson["eventTime"]=datetime.datetime.strptime(str(int(timehrmin.split(":")[0])+12)+":"+timehrmin.split(":")[1],"%H:%M").strftime("%H:%M:%S")
                postJson["eventName"]=MWdf.iloc[i]["Status"]
                postJson["iataCode"]=MWdf.iloc[i]["Location"]
                #if(postJson["eventName"].find(" PU ")!=-1):
                 #   postJson["pickupDate"]=MWdfstatus[]
                eventMap, eventSet = self.getEventConstants(path)

                postJson["eventCode"], postJson["eventName"] = self.Event(eventMap, eventSet,
                                                                          MWdf.iloc[i]['Status'], logger)
                postJson["shipFrom"]=MWdfstatus.loc["Origin:"]["Column-1"]
                postJson["shipFrom"]=postJson["shipFrom"].replace("\xa0","",4)
                postJson["shipTo"]=MWdfstatus.loc["Destination:"]["Column-1"]
                postJson["shipTo"] = postJson["shipTo"].replace("\xa0", "", 4)
                postJson["quantity"]=MWdfstatus.loc["Pieces:"]["Column-1"]
                postJson["weight"]=MWdfstatus.loc["Weight:"]["Column-1"]
                postJson["pickupDate"]=datetime.datetime.strptime(MWdfstatus.loc["Ship Date:"]["Column-1"],"%m/%d/%Y").strftime("%m-%d-%Y")

                if(postJson["currentStatus"].find("Delivered")!=-1):
                    postJson["deliveryDate"]=datetime.datetime.strptime(MWdfstatus.loc["Current Status:"]["Column-1"].split(" ")[0].split("\n")[1],"%m/%d/%Y").strftime("%m-%d-%Y")
                else:
                    postJson["deliveryDate"]=""

                print(postJson)
                with open(path + "\\\\Results\\\\" + pronumber + "resultsStep" + str(i)+".json",'w') as f:
                    json.dump(postJson, f)
                kafka_publisher.publish(postJson)
        except Exception as e:
            logger.info("There was an error in reading from csv. The error is : " + str(e))





def main(pronumberList, cwd, ENV_MODE):
    path = ""
    for x in cwd.split("\\"):
        path += x + "\\\\"
    sys.path.append(path + "\\")

    carrierName = cwd.split("\\")[-1]

    multiprocessPath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-3]])  # till blume multiprocess
    carrierPath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-4]]) + "\\\\" + carrierName
    LTLpath = '\\\\'.join([str(elem) for elem in carrierPath.split("\\\\")[:-1]])
    # till dohrn
    sys.path.append(LTLpath + "\\")
    from LTLCarrierLib import get_config, setup_logger

    pronumberList = list(set(pronumberList))
    config_default = get_config(carrierPath, str(ENV_MODE).strip())
    from KafkaPublisher import KafkaPublisher
    kafka_publisher = KafkaPublisher(server=ast.literal_eval(config_default['kafka_server']),
                                     topic=config_default['kafka_topic'])

    for pro in pronumberList:
        logger = setup_logger(
            pro + "_Logger", multiprocessPath + "\\\\Logs\\\\" + pro + "_LOG.log"
        )

        logger.setLevel(config_default["logger_level"])
        logger.info("Pro Number formatting started")

        post_obj = post()
        # print(i)
        post_obj.EventPost(pro, LTLpath, carrierPath, config_default, logger, kafka_publisher)
    return carrierPath


if __name__ == "__main__":
    #main(['150155697'], "E:\LTLCarrierRPA\Blume_MultiProcess\..\MidWest Motor Express", "dev")
    main(sys.argv[1], sys.argv[2], sys.argv[3])








