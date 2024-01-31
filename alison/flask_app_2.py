# -*- coding: utf-8 -*-
"""
Created on Sun Dec 10 18:47:34 2023

@author: garic
"""

from flask import Flask

app = Flask(__name__)

@app.route("/")
def home():
    return "Hola recomendadores"


if __name__ == "__main__":
    app.run()