"""
Adsinsight BQ API based on Flask
Author: Aashna Shrestha, 2023
Organization: Beam Lab Pvt. Ltd.
"""

from app import app
import app.settings as env

if __name__ == '__main__':
    app.run(host=env.HOST, port=env.PORT)
