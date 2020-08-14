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
import re

sys.path.append("..")


class post:

    def getEventConstants(self, path):
        LTLpath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-1]])  # to get E"://LTL_UIPAth

        eventMap = eval(open(LTLpath + "\\\\EventConstantsMap.txt").read())

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
        pittOhiodf = pd.read_csv(path + "\\\\" + pronumber + ".csv")
        dohrndfservice = pd.read_csv(path+"\\\\"+pronumber + "additional.csv")
        # dohrndfservice = dohrndfservice.rename(columns={"Total:": "Pieces", "Pieces:": "Pallets", "Pallets:": "Weight", "Weight:": "Service Type"})
        pittOhiodfstatus = pd.read_csv(path + "\\\\" + pronumber + "status.csv")
        pittOhiodfstatus = pittOhiodfstatus.set_index("Title")
        pittOhiodf=pittOhiodf.drop(pittOhiodf.index[0])
        sys.path.append(LTLpath + "\\")

        import ShipmentEventBase as baseInfo

        postJson = copy.deepcopy(baseInfo.baseInfo.shipmentEventBase)
        try:
            appointmentData=dohrndfservice.iloc[1]["Column-1"].split("\n")
            aptDateTime=appointmentData[1]
            #print(aptDateTime)
            ## Extract appintment date from string
            match = re.search(r'\d{1}\/\d{2}\/\d{4}', aptDateTime)
            ## if Day is 2 digit
            if(match==None):
                match = re.search(r'\d{2}\/\d{2}\/\d{4}', aptDateTime)
            postJson["appointmentDate"] = datetime.datetime.strptime(match.group(),'%m/%d/%Y').strftime("%Y-%m-%d")
            #print(postJson["appointmentDate"])
            ##extract appointment time start and end from string
            match=re.search(r'\d{1}:\d{2}', aptDateTime)
            matchend=re.search(r' to \d{1}:\d{2}', aptDateTime)
            if(match==None):
                match = re.search(r'\d{2}:\d{2}', aptDateTime)
            postJson["appointmentTimeStart"] = datetime.datetime.strptime(match.group(), "%H:%M").strftime("%H:%M:%S")
            if(aptDateTime.split(" to ")[0].find("pm")!=-1):
                postJson["appointmentTimeStart"]=str(int(postJson["appointmentTimeStart"][:2])+12)+postJson["appointmentTimeStart"][2:]
            #print(match)
            #print(matchend)
            if(matchend==None):
                matchend = re.search(r'\d{2}:\d{2}', aptDateTime)
            postJson["appointmentTimeEnd"] = datetime.datetime.strptime(matchend.group(), ' to %H:%M').strftime("%H:%m:%S")
            if (aptDateTime.split(" to ")[1].find("pm") != -1):
                postJson["appointmentTimeEnd"] = str(int(postJson["appointmentTimeEnd"][:2]) + 12) + postJson["appointmentTimeEnd"][2:]
            #print(postJson["appointmentTimeEnd"])
        except Exception as e:
            print(" The error is : " + str(e))



        for i in range(0,len(pittOhiodf)):
           # print(i)
           # print(pittOhiodf.iloc[i]['Status'])
            if (pittOhiodf.iloc[i]['Status'].find("Delivered")!= -1):
                postJson["deliveryDate"] = pd.datetime.datetime.strptime(pittOhiodf.iloc[i]['Date'],
                                                                         "%m/%d/%Y").strftime("%Y-%m-%d")
                if (pittOhiodf.iloc[i]['Status'].find(" in ") != -1):  # To check if location is mentioned in event
                    postJson["shipTo"] = pittOhiodf.iloc[i]['Status'].split(" in ")[1]

                else:
                    postJson["shipTo"]=""


            elif (pittOhiodf.iloc[i]['Status'].find("Arrived at shipper pickup location")!= -1):
                postJson["pickupDate"] = pd.datetime.datetime.strptime(pittOhiodf.iloc[i]['Date'], "%m/%d/%Y").strftime(
                        "%Y-%m-%d")
                if (pittOhiodf.iloc[i]['Status'].find(" in ") != -1):  # To check if location is mentioned in event
                    postJson["shipFrom"] = pittOhiodf.iloc[i]['Status'].split(" in ")[1]


                else:
                    postJson["shipFrom"]=""

                #   print(postJson["shipTo"])


        # logger.info(dohrndf)
        #print(pittOhiodf.iloc[0])
       # dlvdate= datetime.datetime.strptime(pittOhiodf.iloc[0]['Date'], "%m/%d/%Y").strftime(
       #   "%m-%d-%Y")
       # print(dlvdate)

        for i in range(0,len(pittOhiodf)):
            try:
                logger.info("inside first try for reading config")
                #print(count)
                postJson["uuid"] = str(uuid.uuid4())
                #print(postJson["uuid"])
                postJson["carrierCode"] = config_default["carrier_code"]
                # print(type(postJson["carrierCode"]))
                postJson["carrierName"] = config_default["carrier_name"]
                #   print(postJson["carrierName"])
                postJson["publisherCode"] = config_default["publisher_code"]
                #   print(postJson["publisherCode"])
                postJson["customerName"] = config_default["customer_name"]
                #   print(postJson["customerName"])
                postJson["resolvedEventSource"] = config_default["resolvedEventSource"]
                #print(postJson["resolvedEventSource"]+"1")

            except Exception as e:
                logger.info("There was an error in reading from config file. The error is : " + str(e))

            logger.info("reading from csv")
            postJson["eventType"] = "LTL"
            #print(pittOhiodf)
            #print(pittOhiodf.iloc[1]["Status"])


            try:
               # print(i)

                postJson["proNumber"] = pronumber

                postJson["currentStatus"] = pittOhiodfstatus.loc["Status:"]["Data"]
               # print(postJson["currentStatus"])

                #  print(postJson["deliveryDate"])

                # postJson["trailerNumber"] = yrcdf.loc["Trailer #:"]['Data']

                eventMap, eventSet = self.getEventConstants(path)

                if(pittOhiodf.iloc[i]['Status'].find(" in ")!=-1):
                    postJson["eventName"]=pittOhiodf.iloc[i]['Status'].split(" in ")[0]
                else:
                    postJson["eventName"] = pittOhiodf.iloc[i]['Status']
                #print(postJson["eventName"])



                postJson["eventCode"], postJson["eventName"] = self.Event(eventMap, eventSet,postJson["eventName"]
                                                                          , logger)
               # print(postJson["eventCode"])
                logger.info(postJson["eventCode"])
                logger.info(postJson["eventName"])
                postJson["signedBy"]=""
                postJson["trailerNumber"]=""
                postJson["quantity"]=""
                postJson["weight"]=""

                print(postJson["eventName"])
                postJson["eventDate"] = datetime.datetime.strptime(pittOhiodf.iloc[i]['Date'], "%m/%d/%Y").strftime(
                    "%m-%d-%Y")
                #print(postJson["eventDate"])
                #   print(postJson["eventDate"])
               # print(type(pittOhiodf.loc[i]['Time']))
               # print(pittOhiodf.loc[i]['Time'])
                if(pittOhiodf.iloc[i]['Time'].find("AM")!=-1):
                    postJson["eventTime"] = pd.datetime.datetime.strptime(pittOhiodf.iloc[i]['Time'],"%H:%M AM").strftime("%H:%M:%S")
                else:
                    hour=pittOhiodf.iloc[i]['Time'].split(":")[0]
                    #print(hour)
                    time_24hr=str(int(hour)+12)+":"+pittOhiodf.iloc[i]['Time'].split(":")[1]
                    #print(time_24hr)
                    postJson["eventTime"] = pd.datetime.datetime.strptime(time_24hr, "%H:%M PM").strftime("%H:%M:%S")


               # print(postJson["eventTime"])


                # postJson["signedBy"] = yrcdf.loc["Status:"]['Data'].split("-")[1].split(":")[1].strip()
                print(postJson)
                with open(path + "\\\\Results\\\\" + pronumber + "resultsStep" + str(i) + ".json",
                          'w') as f:
                    json.dump(postJson, f)
            except Exception as e:
                logger.info("There was an error in reading from csv. The error is : " + str(e))
            #kafka_publisher.publish(postJson)


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
    #main(sys.argv[1], sys.argv[2], sys.argv[3])
    main(['5030432279'], 'E:\LTLCarrierRPA\Blume_MultiProcess\..\PittOhio', "dev")









