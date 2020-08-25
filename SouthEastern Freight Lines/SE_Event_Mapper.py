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
       # print(LTLpath)

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
        SEdf = pd.read_csv(path + "\\\\" + pronumber + ".csv")
        #print(SEdf)
        SEdf1=SEdf[["Column-0","Column-1"]]
        SEdf1.dropna(subset=["Column-0"], inplace=True)
        SEdf1.fillna('', inplace=True)
        SEdf1.set_index("Column-0",inplace=True)

        SEdf2 = SEdf[["Column-2", "Column-3"]]

        SEdf2.dropna(subset=["Column-2"], inplace=True)
        SEdf2.fillna('', inplace=True)
        SEdf2.set_index("Column-2",inplace=True)
       # print(SEdf2)
       # print(SEdf2.loc["Status:"]["Column-3"].split(" ")[0])
       # print(SEdf1.loc["Status:"]["Column-1"].split(" ")[0])
        sys.path.append(LTLpath + "\\")

        import ShipmentEventBase as baseInfo

        postJson = copy.deepcopy(baseInfo.baseInfo.shipmentEventBase)
        try:
            logger.info("inside first try for reading config")
            # print(count)
            postJson["uuid"] = str(uuid.uuid4())
            # print(postJson["uuid"])
            postJson["carrierCode"] = config_default["carrier_code"]
            # print(type(postJson["carrierCode"]))
            postJson["carrierName"] = config_default["carrier_name"]
            #   print(postJson["carrierName"])
            postJson["publisherCode"] = config_default["publisher_code"]
            #   print(postJson["publisherCode"])
            postJson["customerName"] = config_default["customer_name"]
            #   print(postJson["customerName"])
            postJson["resolvedEventSource"] = config_default["resolvedEventSource"]
            # print(postJson["resolvedEventSource"]+"1")

        except Exception as e:
            logger.info("There was an error in reading from config file. The error is : " + str(e))

        logger.info("reading from csv")
        postJson["eventType"] = "LTL"
        try:
            # print(i)

            postJson["proNumber"] = SEdf1.loc["Pro Number:"]["Column-1"]
            postJson["quantity"]=SEdf1.loc["Pieces:"]["Column-1"]
            postJson["weight"]=SEdf1.loc["Weight:"]["Column-1"]
           # print(SEdf2.loc["Pickup Date/Time:"]["Column-3"].split(" ")[0])
            postJson["pickupDate"]=datetime.datetime.strptime(SEdf2.loc["Pickup Date/Time:"]["Column-3"].split(" ")[0],"%m/%d/%Y").strftime("%m-%d-%Y")
          #  print(postJson["pickupDate"])
            postJson["signedBy"]=SEdf2.loc["Consignee:"]["Column-3"]
            postJson["currentStatus"]=SEdf2.loc["Status:"]["Column-3"].split(" ")[0]
            if(postJson["currentStatus"].find("Delivered")!=-1):
                postJson["deliveryDate"]=datetime.datetime.strptime(SEdf2.loc["Status:"]["Column-3"].split(" ")[1],"%m/%d/%Y").strftime("%m-%d-%Y")
            else:
                postJson["deliveryDate"] =""
            eventMap, eventSet = self.getEventConstants(path)
            postJson["eventCode"], postJson["eventName"] = self.Event(eventMap, eventSet, postJson["currentStatus"]
                                                                      , logger)
            postJson["eventDate"]=datetime.datetime.strptime(SEdf2.loc["Status:"]["Column-3"].split(" ")[1],"%m/%d/%Y").strftime("%m-%d-%Y")
            with open(path + "\\\\Results\\\\" + pronumber + "resultsStep" + ".json",
                      'w') as f:
                json.dump(postJson, f)
        except Exception as e:
            logger.info("There was an error in reading from csv. The error is : " + str(e))
        # kafka_publisher.publish(postJson)








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
    #main(['63708898-1'],"E:\LTLCarrierRPA\Blume_MultiProcess\..\SouthEastern Freight Lines","dev")
    main(sys.argv[1], sys.argv[2], sys.argv[3])








