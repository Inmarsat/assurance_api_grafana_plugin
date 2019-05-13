import json
import requests
import os
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, abort
from flask_cors import CORS, cross_origin
from datetime import datetime

app = Flask(__name__)
cors = CORS(app)

PORT_NUMBER = 5001 #Flask server will be running at http://localhost:PORT_NUMBER/
METHODS = ('GET', 'POST') #API Methods used

def to_epoch(dt_format):#converts grafana timestamp to epoch in nanoseconds
    epoch = int((datetime.strptime(dt_format, "%Y-%m-%dT%H:%M:%S.%fZ") - datetime(1970, 1, 1)).total_seconds() * 1e9)
    return epoch

class API_Request(object):
    #object of type API_Request. .make_request() method makes the API request to REST API. Has variables needed to make the request

    def __init__(self, x_api_key):
        self.baseurl = "https://inmarsat-prod.apigee.net/v1/fleetEdge/assurance/"
        self.x_api_key = x_api_key
        self.limit = 10000

        #create metric_lookup_df
        response = self.make_request("metric/")

        metric_lookup_df = pd.DataFrame.from_dict(response)

        device_type = metric_lookup_df["device_type"].to_list()
        metric_name = metric_lookup_df["metric_name"].to_list()
        metric_lookup_df.index = [device_type[ii] + "_" + metric_name[ii] for ii in range(0,len(metric_name))]
        self.metric_lookup_df = metric_lookup_df

    def make_request(self, endpoint, **kwargs):

        querystring = kwargs #dictionary kwargs forms basis for querystring
        api_url = self.baseurl + endpoint #the url to query is the base url + endpoint

        try:
            querystring["date_time.gte"] = querystring.pop("start_time")#rename start_time dictionary key as date_time.gte
        except KeyError:
            pass
        try:
            querystring["date_time.lte"] = querystring.pop("end_time")#rename end_time dictionary key as date_time.lte
        except KeyError:
            pass
        try:
            metric_name = querystring.pop("metric_name")
            querystring["metric_id"] = self.metric_lookup_df.loc[metric_name,"metric_id"] #lookup metric_id in metric_lookup_df
        except KeyError:
            pass
        querystring["limit"] = self.limit
        headers = {'x-api-key':  self.x_api_key} #API key for VAR

        response_class = requests.request("GET", api_url, headers=headers, params=querystring) #GET request to API
        if response_class.status_code != 200: #if the status code is not 200, raise an error
            raise ValueError("REST API returning status code " + str(response_class.status_code))

        response = json.loads(response_class.text) #convert JSON string to dictionary

        return response

@app.route('/', methods=METHODS)#Needs to return 200 'OK'
@cross_origin() #The browser will accept responses from a different origion, i.e. a different server, running on a different port number
def return_ok():
    try: #try authorisation
        auth = request.authorization  #authentication header
        x_api_key = auth.password #x_api_key contained as password
        global api_request
        api_request = API_Request(x_api_key = x_api_key) #Create an instance of the class API_Request, in the global enviroment
        return "Ok"
    except AttributeError:
        abort(403)

@app.route('/search',methods=METHODS)#Needs to return a list of avaliable metrics
@cross_origin()
def search_route():
    return json.dumps(api_request.metric_lookup_df.index.to_list())

@app.route('/query',methods=METHODS) #Needs to return assurence data
@cross_origin()
def query_route():
    req = request.get_json() #parse and returns the POST request as JSON, returns in dictionary form

    #Extract the from and to timestamps from Req dict, and convert to EPOCH timestamp in nanoseconds
    range_from = to_epoch(req["range"]["from"])
    range_to = to_epoch(req["range"]["to"])

    freq = req["intervalMs"] #frequency of sampeling in miliseconds
    limit = req["maxDataPoints"] #Maximum number of datapoints to return
    edge_ids = []

    for dictionary in req["adhocFilters"]:#get the x-api-key and edge ID
        if dictionary["key"] == "EdgeID":
            edge_ids.append(dictionary["value"]) #Assign to the global variable edge_id

    types = [dictionary["type"] for dictionary in req["targets"]]
    metric_names = [dictionary["target"] for dictionary in req["targets"]]
    
    if all(Type == "timeserie" for Type in types):
        response_to_return = []
        for edge_id in edge_ids:

            for metric_name in metric_names:

                response = api_request.make_request(endpoint = "status/", metric_name = metric_name, start_time = range_from, end_time = range_to, device_id = edge_id)

                timestamps = [int(datapoint["timestamp"]/1000000) for datapoint in response] #convert timestamps to int, and into miliseconds
                values = [datapoint["value"] for datapoint in response] #convert values to floats

                datapoints = [[values[i], timestamps[i]] for i in range(0,len(values))]

                response_to_return.append({"target": edge_id + "_" + metric_name, "datapoints": datapoints, "edge_id": edge_id, "metric_name": metric_name})
        return jsonify(response_to_return)
            
    elif all(Type == "table" for Type in types): #table formatting
        response_to_return = {}
        response_to_return["name"] = "TableResponse"
        response_to_return["type"] = "table"
        response_to_return["columns"] = [{"text":"time"},{"text":"edge_id"}]
        response_to_return["rows"] = []

        for metric_name in metric_names:
            response_to_return["columns"].append({"text":metric_name})
        
        for edge_id in edge_ids:
            is_first_iteration = True
            rows = []
            for metric_name in metric_names:

                response = api_request.make_request(endpoint = "status/", metric_name = metric_name, start_time = range_from, end_time = range_to, device_id = edge_id)

                timestamps = [int(datapoint["timestamp"]/1000000) for datapoint in response] #convert timestamps to int, and into miliseconds
                values = [datapoint["value"] for datapoint in response] #convert values to floats

                if is_first_iteration:
                    for n in range(len(timestamps)):
                        rows.append([timestamps[n],edge_id,values[n]])
                elif not is_first_iteration:
                    for n in range(len(timestamps)):
                        rows[n].append(values[n])

                is_first_iteration = False
            response_to_return["rows"].extend(rows)

        return jsonify([response_to_return])

    else:
        abort(400)
 
@app.route('/tag-keys',methods=METHODS) #Needs to return the avaliable ad hoc filters.
@cross_origin()
def tag_keys_route():
    return json.dumps([{"type":"string","text":"EdgeID"}])

@app.route('/annotations',methods=METHODS)
@cross_origin()
def annotations_route():
    req = request.get_json() #POST request in dictionary form
    annotation = req["annotation"] #annotation part of the request
    timestamp = datetime.utcnow().timestamp()* 1000 #current EPOCH timestamp in milliseconds

    return jsonify(time = timestamp, title = "Simple JSON datasource", annotation = annotation)

@app.route('/tag-values',methods=METHODS) #Needs to retun the avaliable values for the adhoc filters
@cross_origin()
def tag_values_route():
    req = request.get_json()
    key = req["key"]
    if key == "EdgeID":
        response = api_request.make_request(endpoint = "status/")

        avaliable_edge_ids = np.unique([dictionary["device_id"] for dictionary in response]).tolist()
        response = [{"text" : edge_id} for edge_id in avaliable_edge_ids]
        return json.dumps(response)
    else:
        return json.dumps([])

app.run(host='localhost', port = PORT_NUMBER,debug = True) 
#runs on http://localhost:PORT_NUMBER/