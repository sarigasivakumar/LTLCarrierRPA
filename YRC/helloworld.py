import sys
import os
import json
import imp

import configparser
from kafka import KafkaProducer, errors






def main(pro,cwd,str1):
    path = ""
    for x in cwd.split("\\"):
        path += x + "\\\\"
    sys.path.append(path + "\\")
    thisproj=cwd.split("\\")[-1]

    basePath = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-3]])#till bl

    Pathhere = '\\\\'.join([str(elem) for elem in path.split("\\\\")[:-4]])+"\\\\"+thisproj

    pathnew = '\\\\'.join([str(elem) for elem in Pathhere.split("\\\\")[:-1]])#till ltl

    sys.path.append(basePath)
    print("hello world")
    dir=os.getcwd()

    eventMap = eval(open(pathnew+ "\\\\EventConstantsMap.txt").read())
    f=open(Pathhere+"\\Mapcontent.json","w")
    json.dump(eventMap,f)
    f.close()
    '''
    try:
        fp, path, desc = imp.find_module("LTLCarrierLib")

    except ImportError:
        print("module not found: " + "LTLCarrierLib")

    try:
        # load_modules loads the module
        # dynamically ans takes the filepath
        # module and description as parameter
        LTLPackage = imp.load_module("LTLCarrierLib", fp,
                                          path, desc)

    except Exception as e:
        print(e)
        '''

    sys.path.append(pathnew + "\\")
    from LTLCarrierLib import get_config
    config_default=get_config(Pathhere, str(str1).strip())
    pronumberList = list(set(pro))
    #config = configparser.ConfigParser()
    #config.read(Pathhere + "\\Config\\config-%s.ini" % str(str1))
    #config_default=config["DEFAULT"]



    f=open(pathnew+"\\Mapcontent1.txt","w") #outside yrc
    for pro in pronumberList:
        f.write(pro)
    f.close()
    return pathnew








if __name__ == "__main__":
    # testMain(sys.argv[1])
    main(sys.argv[1],sys.argv[2])#,sys.argv[3])