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
        ABFdf = pd.read_csv(path + "\\\\" + pronumber + ".csv")
        #print(SEdf)
        #SEdf1=SEdf[["Column-0","Column-1"]]

        ABFdfevent=pd.read_csv(path + "\\\\" + pronumber + "event.csv")
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
        try:
            postJson["proNumber"]=pronumber
            postJson["eventType"] = "LTL"
            print(ABFdf)
            postJson["quantity"]=ABFdf.iloc[0]["Data"].str
            postJson["weight"]=ABFdf.iloc[1]["Data"].str
            print(ABFdfevent)
            postJson["currentStatus"]=ABFdfevent.iloc[0]["Data"].split(":")[1].strip()
            eventMap, eventSet = self.getEventConstants(path)
            postJson["eventName"]=postJson["currentStatus"]
            postJson["eventCode"], postJson["eventName"] = self.Event(eventMap, eventSet,
                                                                      postJson["eventName"], logger)
            yr=ABFdfevent.iloc[1]["Data"].split("ON")[1][7:9]
            #print(yr)
            postJson["deliveryDate"]=ABFdfevent.iloc[1]["Data"].split("ON")[1][0:7]+"20"+yr

            postJson["deliveryDate"] =datetime.datetime.strptime(postJson["deliveryDate"].strip(),"%m/%d/%Y").strftime("%m-%d-%Y")
           # print(postJson)
        except Exception as e:
            logger.info("There was an error in reading from config file. The error is : " + str(e))
        kafka_publisher.publish(postJson)









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
    main(['071703571'],"E:\LTLCarrierRPA\Blume_MultiProcess\..\ABF Freight","dev")
    #main(sys.argv[1], sys.argv[2], sys.argv[3])








