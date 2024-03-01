"""
Report Analytics Routes
Author: Nikesh Bhattarai, 2024
Organization: Beam Lab Pvt. Ltd.
"""
from __future__ import division, print_function, absolute_import

import traceback
from datetime import datetime
from google.cloud import bigquery
from flask import jsonify, request, Blueprint

from app.util import verify_headers

report = Blueprint('report', __name__)
client = bigquery.Client()

@report.route('/', methods=['POST'], strict_slashes=False)
def reachSummary():
    error, code, msg = verify_headers(request.headers)
    if error:
        message = {
            'error' : {
                'code' : code,
                'message' : msg
            }
        }
        response = jsonify(message)
        response.status_code = code
        return response
    try:
        from_date = str(request.json.get("from_date"))
        to_date = str(request.json.get("to_date"))
        campaign_id = str(request.json.get("campaign_id"))
        client_id = str(request.json.get("client_id"))
        advertisement_id = str(request.json.get("advertisement_id"))
        query = (
            'SELECT '
            'A.clientId, '
            'A.campaignId, '
            'A.campaignName, '
            'A.advertisementId, '
            'A.advertisementName, '
            'COUNT(A.clientId) as adCount '
            'FROM '
            '`adsinsight.ads.playlist` A '
            'WHERE '
            'DATE(A.playedAt) >= DATE("' + from_date + '") '
            'AND DATE(A.playedAt) <= DATE("' + to_date + '") '
            'AND A.campaignId = ' + campaign_id + ' '
            'AND A.clientId = ' + client_id + ' '
            'AND A.advertisementId = ' + advertisement_id + ' '
            'GROUP BY '
            'A.clientId, A.campaignId, A.campaignName, A.advertisementId, A.advertisementName'
        )
        query_job = client.query(
            query,
            location="US",
        )
        ad_data = []
        for row in query_job:
            record = {}
            keys = list(row.keys())
            for key in keys:
                record[key] = str(row[key]) if str(row[key]) != "None" else "0"
            ad_data.append(record)
        query = (
            'SELECT '
            'A.clientId, '
            'A.campaignId, '
            'A.campaignName, '
            'A.advertisementId, '
            'A.advertisementName, '
            'COUNT(A.trackerId) as vehicleCount '
            'FROM '
            '`adsinsight.ads.traffic` A '
            'WHERE '
            'DATE(A.recordedAt) >= DATE("' + from_date + '") '
            'AND DATE(A.recordedAt) <= DATE("' + to_date + '") '
            'AND A.campaignId = ' + campaign_id + ' '
            'AND A.clientId = ' + client_id + ' '
            'AND A.advertisementId = ' + advertisement_id + ' '
            'GROUP BY '
            'A.clientId, A.campaignId, A.campaignName, A.advertisementId, A. advertisementName'
        )
        query_job = client.query(
            query,
            location="US",
        )
        traffic_data = []
        for row in query_job:
            record = {}
            keys = list(row.keys())
            for key in keys:
                record[key] = str(row[key]) if str(row[key]) != "None" else "0"
            traffic_data.append(record)
        message = {
            'ad': ad_data,
            'traffic': traffic_data
        }
        response = jsonify(message)
        response.status_code = 200
        return response
    except Exception as inst:
        print(traceback.print_exc())
        message = {
            'error' : {
                'code' : 404,
                'message' : str(inst)
            }
        }
        response = jsonify(message)
        response.status_code = 404
        return response

@report.route('/daily', methods=['POST'])
def dailyReach():
    error, code, msg = verify_headers(request.headers)
    if error:
        message = {
            'error' : {
                'code' : code,
                'message' : msg
            }
        }
        response = jsonify(message)
        response.status_code = code
        return response

    try:
        from_date = str(request.json.get("from_date"))
        to_date = str(request.json.get("to_date"))
        campaign_id = str(request.json.get("campaign_id"))
        client_id = str(request.json.get("client_id"))
        advertisement_id = str(request.json.get("advertisement_id"))
        query = (
            'SELECT '
            'DATE_TRUNC(DATE(A.recordedAt), DAY) AS date, '
            'COUNT(A.trackerId) AS totalIn '
            'FROM '
            '`adsinsight.ads.traffic` A '
            'WHERE '
            'DATE(A.recordedAt) >= DATE("' + from_date + '") '
            'AND DATE(A.recordedAt) <= DATE("' + to_date + '") '
            'AND A.campaignId = ' + campaign_id + ' '
            'AND A.clientId = ' + client_id + ' '
            'AND A.advertisementId = ' + advertisement_id + ' '
            'GROUP BY '
            'date '
            'ORDER BY '
            'date ASC'
        )
        print(query)
        query_job = client.query(
            query,
            location="US",
        )
        data = []
        for row in query_job:
            record = {}
            keys = list(row.keys())
            for key in keys:
                record[key] = str(row[key]) if str(row[key]) != "None" else "0"
            data.append(record)
        message = {
            'data': data,
        }
        response = jsonify(message)
        response.status_code = 200
        return response
    except Exception as inst:
        print(traceback.print_exc())
        message = {
            'error' : {
                'code' : 404,
                'message' : str(inst)
            }
        }
        response = jsonify(message)
        response.status_code = 404
        return response

@report.route('/day', methods=['POST'])
def reachByDay():
    error, code, msg = verify_headers(request.headers)
    if error:
        message = {
            'error' : {
                'code' : code,
                'message' : msg
            }
        }
        response = jsonify(message)
        response.status_code = code
        return response

    try:
        from_date = str(request.json.get("from_date"))
        to_date = str(request.json.get("to_date"))
        campaign_id = str(request.json.get("campaign_id"))
        client_id = str(request.json.get("client_id"))
        advertisement_id = str(request.json.get("advertisement_id"))
        query = (
            'SELECT '
            'DATE_TRUNC(DATE(A.recordedAt), DAY) AS date, '
            'FORMAT_DATE("%A", DATE_TRUNC(DATE(A.recordedAt), DAY)) AS day, '
            'COUNT(A.trackerId) AS totalIn '
            'FROM '
            '`adsinsight.ads.traffic` A '
            'WHERE '
            'DATE(A.recordedAt) >= DATE("' + from_date + '") '
            'AND DATE(A.recordedAt) <= DATE("' + to_date + '") '
            'AND A.campaignId = ' + campaign_id + ' '
            'AND A.clientId = ' + client_id + ' '
            'AND A.advertisementId = ' + advertisement_id + ' '
            'GROUP BY '
            'date, day '
            'ORDER BY '
            'date ASC'
        )
        query_job = client.query(
            query,
            location="US",
        )
        data = []
        for row in query_job:
            record = {}
            keys = list(row.keys())
            for key in keys:
                record[key] = str(row[key]) if str(row[key]) != "None" else "0"
            data.append(record)
        message = {
            'data': data,
        }
        response = jsonify(message)
        response.status_code = 200
        return response
    except Exception as inst:
        print(traceback.print_exc())
        message = {
            'error' : {
                'code' : 404,
                'message' : str(inst)
            }
        }
        response = jsonify(message)
        response.status_code = 404
        return response


@report.route('/time', methods=['POST'])
def reachByTime():
    error, code, msg = verify_headers(request.headers)
    if error:
        message = {
            'error' : {
                'code' : code,
                'message' : msg
            }
        }
        response = jsonify(message)
        response.status_code = code
        return response

    try:
        from_date = str(request.json.get("from_date"))
        to_date = str(request.json.get("to_date"))
        campaign_id = str(request.json.get("campaign_id"))
        client_id = str(request.json.get("client_id"))
        advertisement_id = str(request.json.get("advertisement_id"))
        query = (
            'SELECT '
            'DATE_TRUNC(DATE(A.recordedAt), DAY) AS date, '
            'TIME_TRUNC(TIME(A.recordedAt), HOUR) AS time, '
            'FORMAT_DATE("%A", DATE_TRUNC(DATE(A.recordedAt), DAY)) AS day, '
            'COUNT(A.trackerId) AS totalIn '
            'FROM '
            '`adsinsight.ads.traffic` A '
            'WHERE '
            'DATE(A.recordedAt) >= DATE("' + from_date + '") '
            'AND DATE(A.recordedAt) <= DATE("' + to_date + '") '
            'AND A.campaignId = ' + campaign_id + ' '
            'AND A.clientId = ' + client_id + ' '
            'AND A.advertisementId = ' + advertisement_id + ' '
            'GROUP BY '
            'date, time, day '
            'ORDER BY '
            'date, time ASC'
        )
        query_job = client.query(
            query,
            location="US",
        )
        data = []
        for row in query_job:
            record = {}
            keys = list(row.keys())
            for key in keys:
                record[key] = str(row[key]) if str(row[key]) != "None" else "0"
            data.append(record)
        message = {
            'data': data,
        }
        response = jsonify(message)
        response.status_code = 200
        return response
    except Exception as inst:
        print(traceback.print_exc())
        message = {
            'error' : {
                'code' : 404,
                'message' : str(inst)
            }
        }
        response = jsonify(message)
        response.status_code = 404
        return response


@report.route('/vehicletype', methods=['POST'])
def vehicleTypeInsights():
    error, code, msg = verify_headers(request.headers)
    if error:
        message = {
            'error': {
                'code': code,
                'message': msg
            }
        }
        response = jsonify(message)
        response.status_code = code
        return response

    try:
        from_date = str(request.json.get("from_date"))
        to_date = str(request.json.get("to_date"))
        campaign_id = str(request.json.get("campaign_id"))
        client_id = str(request.json.get("client_id"))
        advertisement_id = str(request.json.get("advertisement_id"))

        query = (
            'SELECT '
            'vehicleType, '
            'COUNT(trackerId) as vehicleCount '
            'FROM '
            '`adsinsight.ads.traffic` '
            'WHERE '
            'DATE(recordedAt) >= DATE("' + from_date + '") '
            'AND DATE(recordedAt) <= DATE("' + to_date + '") '
            'AND campaignId = ' + campaign_id + ' '
            'AND clientId = ' + client_id + ' '
            'AND advertisementId = ' + advertisement_id + ' '
            'GROUP BY '
            'vehicleType '
            'ORDER BY '
            'vehicleType'
        )

        query_job = client.query(query, location="US")

        data = []
        for row in query_job:
            record = {}
            keys = list(row.keys())
            for key in keys:
                record[key] = str(row[key]) if str(row[key]) != "None" else "0"
            data.append(record)

        message = {'data': data}
        response = jsonify(message)
        response.status_code = 200
        return response
    except Exception as inst:
        print(traceback.print_exc())
        message = {
            'error': {
                'code': 404,
                'message': str(inst)
            }
        }
        response = jsonify(message)
        response.status_code = 404
        return response
    

@report.route('/dwelltime', methods=['POST'])
def dwellTimeInsights():
    error, code, msg = verify_headers(request.headers)
    if error:
        message = {
            'error': {
                'code': code,
                'message': msg
            }
        }
        response = jsonify(message)
        response.status_code = code
        return response

    try:
        from_date = str(request.json.get("from_date"))
        to_date = str(request.json.get("to_date"))
        campaign_id = str(request.json.get("campaign_id"))
        client_id = str(request.json.get("client_id"))
        advertisement_id = str(request.json.get("advertisement_id"))

        query = (
            'SELECT '
            'vehicleType, '
            'AVG(timeSpent) as avg_time_spent '
            'FROM '
            '`adsinsight.ads.traffic` '
            'WHERE '
            'DATE(recordedAt) >= DATE("' + from_date + '") '
            'AND DATE(recordedAt) <= DATE("' + to_date + '") '
            'AND campaignId = ' + campaign_id + ' '
            'AND clientId = ' + client_id + ' '
            'AND advertisementId = ' + advertisement_id + ' '
            'GROUP BY '
            'vehicleType '
            'ORDER BY '
            'vehicleType'
        )

        query_job = client.query(query, location="US")

        data = []
        for row in query_job:
            record = {}
            keys = list(row.keys())
            for key in keys:
                record[key] = str(row[key]) if str(row[key]) != "None" else "0"
            data.append(record)

        message = {'data': data}
        response = jsonify(message)
        response.status_code = 200
        return response
    except Exception as inst:
        print(traceback.print_exc())
        message = {
            'error': {
                'code': 404,
                'message': str(inst)
            }
        }
        response = jsonify(message)
        response.status_code = 404
        return response