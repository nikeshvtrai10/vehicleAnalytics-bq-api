"""
Blueprints for API Routes
Author: Nikesh Bhattarai, 2024
Organization: Beam Lab Pvt. Ltd.
"""

from __future__ import division, print_function, absolute_import

from flask import Flask, request, jsonify
from flask_cors import CORS

# create the flask app
app = Flask(__name__)

# cors
CORS(app)

# Register blueprints for different API routes
from app.blueprints.overview import overview
from app.blueprints.report import report

app.register_blueprint(overview, url_prefix='/overview')
app.register_blueprint(report, url_prefix='/report')

# API endpoint for root path
@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def home():
    """ Home route
    """
    message = {
        'data': {
            'message': 'This is the API endpoint for Adsinsight Analytics Solution.'
        }
    }
    response = jsonify(message)
    response.status_code = 200
    return response

# Error Handlers
@app.errorhandler(404)
def not_found(error=None):
    """ 404 Route
    """
    message = {
        'error': {
            'code': 404,
            'message': 'Not Found: ' + request.url,
        }
    }
    response = jsonify(message)
    response.status_code = 404
    return response

@app.errorhandler(405)
def not_allowed(error=None):
    """ 405 Not Allowed Route
    """
    message = {
        'error': {
            'code': 405,
            'message': 'Method Not Allowed: ' + request.method,
        }
    }
    response = jsonify(message)
    response.status_code = 405
    return response
