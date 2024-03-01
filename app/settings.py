"""
Settings for python dotenv
Author: Aashna Shrestha, 2023
Organization: Beam Lab Pvt. Ltd.
"""

import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
dotenv_path = os.path.join(basedir, '.env')
load_dotenv(dotenv_path)

HOST = os.environ.get("HOST")
PORT = int(os.environ.get("PORT"))
