# Defines functions used across multiple webscraping scripts

import os
import sys
import logging
import requests
import json
import pandas
import time
import math
import configparser

from bs4 import BeautifulSoup
from threading import Thread


formatter = logging.Formatter(
    "%(asctime)s , [%(levelname)s]: %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
)
########## CONFIG FUNCTIONS ###########
def get_config(mypath: str="", env_mode: str="local"):
    #ENV_MODE = os.environ.get("ENV_MODE")
    config = configparser.ConfigParser()
    try:
        #config.read(mypath+"config-%s.ini" %ENV_MODE)
        config.read(mypath+"\\Config\\config-%s.ini" %env_mode)
        print(mypath+"\\Config\\config-%s.ini" %env_mode)
        #print(config)
        return config['DEFAULT']
    except Exception as e:
        print("Cannot read configuration file:", e)
        sys.exit(0)

########## GENERAL FUNCTIONS ##########

def generate_jwt_token(url, user):
    response = requests.get(url+user)
    info = json.loads(response.text)
    return info['token']


def directory_setup(directory: str):
    """Creates the directory (for json files) if it doesn't exist, or clear all existing files in the directory
    if it does"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    # clear local directory
    filelist = [f for f in os.listdir(directory) if f.endswith(".json")]
    for f in filelist:
        os.remove(os.path.join(directory, f))




def setup_logger(name, log_file):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    return logger

def dump_json(obj: dict, directory: str):
    """Write vessel information to json file"""
    try:
        if "vesselCode" in obj["vesselInfo"]:
            file_name = obj["vesselInfo"]["vesselCode"] + "_" + obj["vesselInfo"]["voyageNumber"]
        else:
            file_name = obj["vesselInfo"]["vesselName"].replace(" ", "-") + "_" + obj["vesselInfo"]["voyageNumber"]
        with open(directory + "/" + file_name + ".json", 'w') as outfile:
            json.dump(obj, outfile)
    except IOError as e:
        logging.error("dump_json: " + e.message)


def log_exception_wrapper(func_name: str, func, *args):
    """Calls the given function in a try/except block so any exception that occurs will be logged"""
    try:
        return func(*args)
    except Exception as e:
        logging.error(func_name + ": " + str(e))

def getEventConstants():
    eventMap = eval(open("../EventConstantsMap.txt").read())
    eventMap = {k.lower(): v for k, v in eventMap.items()}
    return eventMap

def get_awbs(url, configAWBs, logger):
    awbs = []
    res=requests.get(url)
    #print("status_code", res.status_code)
    if res.status_code != 200:
        logger.info("API call failed; Reading AWBs from config file")
    else:
        content = res.json()
        try:
            awbs = [x['airWayBillNumber'] for x in content]
        except Exception as e:
            logger.info("API response empty list of AWBs; Reading AWBs from config file" + str(e))
    # if no awb from api, return awbs from config file
    if not awbs:
        awbs = list(set(configAWBs.split(",")))
    return awbs
