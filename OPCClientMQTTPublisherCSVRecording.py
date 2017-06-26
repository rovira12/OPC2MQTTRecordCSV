#!/usr/bin/python

##########################################################################################
##########################################################################################
##############                                         ###################################
############## OPC Client//MQTT Publish//CSV Recording ###################################
##############                                         ###################################
##########################################################################################
##########################################################################################

import time
import csv
import OpenOPC
import sys
import os
import inspect
import paho.mqtt.publish as publish

def importOPC(OPCserver,taglist):
    message=[]
    try:
        opc.connect(OPCserver1)         #Connection with the OPC Server
    except:
        print('Unable to connect with the OPC Server')

    try:
        v = opc.read(tags=taglist)         #Reading tags from OPC
        str(v)
        for i in range(len(v)):
            (name,val,qual,time)=v[i]
            if val =='':
                val = 0
            message.insert(i,val)
        message.insert(0,time)
        print(message)
    except:
        print('Unable to get the values')
    return message 

def AddItemsCSV(OPCserver,CSVname,taglist):
    """
    Connect OPCserver
    Extract a value of the value
    of the taglist and append them to
    liste and to csv
    taglist: list with name of all items
    liste: list of item value
    """

    attach = [] #Initialize attach
    
    try:
        opc = OpenOPC.client()                #Connection with the OPC Server
        opc.connect(OPCserver)
    except:
        print("No connection to Server")
        
    try:    
        v = opc.read(tags=taglist)         #Reading tags from OPC
        str(v)
    except:
        print("Unable to read tags")

    try:
        
        
        # Create a line with the values from the tag
        
        for i in range(len(v)):
            (name, val, qual, time) = v[i]
            if val == '':
                val = 0
            attach.insert(i,val)
        attach.insert(0,time)
    except:
        print('Unable to create a row')

    #Writing the line to the CSV file

    try:        
        b = open(CSVname,'ab')
        a = csv.writer(b)
        a.writerow(attach)
        b.close()
    except:
        print('Unable to write in CSV')


#Initialize values

OPCserver1='NewportOPC.iSeries'
CSVname1='Newport.csv'
taglist1=['147_83_208_109.UWTC02.Temperature1','147_83_208_109.UWTC02.Temperature2','147_83_208_109.UWTC02.Battery']
variables=['Time','Temperature1','Temperature2','Battery']
publishlist=['Lab/Time','Lab/Temp1','Lab/Temp2','Lab/Bat']
UpdateRate=30

opc = OpenOPC.client()

#Creating the first row with the names of the variables

try:
    b = open(CSVname1,'ab')
    a = csv.writer(b)
    a.writerow(variables)
    b.close()
except:
    print('Unable to initialize the CSV file')

#Main program

while True:
    #CSV Function
    AddItemsCSV(OPCserver=OPCserver1,CSVname=CSVname1,taglist=taglist1)

    #Getting information from the OPC server

    message = importOPC(OPCserver1,taglist1)

    #MQTT Publish
    
    for i in range(len(message)):
        top=publishlist[i]
        value=message[i]
        publish.single(topic=top, payload=value, hostname="localhost")

    #Delay UpdateRate
    
    time.sleep(UpdateRate)
 
