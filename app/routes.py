# coding=utf-8

from flask import render_template, flash, redirect, session, url_for, request, g, Markup, Flask, jsonify, Request
from app import app
from business_rules_utils import *
import json
from flask_cors import CORS
import ast
from datetime import date,datetime


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/get_data')
def get_data_route():
    params = request.json
    tenant_id = params.get('tenant_id', None)
    database = params.get('database', None)
    table = params.get('table', None)
    case_id = params.get('case_id', None)
    case_id_based = params.get('case_id_base', True)
    view = params.get('view', 'records')
    result = get_data(tenant_id, database, table, case_id, case_id_based=True, view='records')
    return jsonify(result)

@app.route('/save_data')
def save_data_route():
    params = request.json
    tenant_id = params.get('tenant_id', None)
    database = params.get('database', None)
    table = params.get('table', None)
    data = params.get('data', None)
    case_id = params.get('case_id', None)
    case_id_based = params.get('case_id_base', True)
    view = params.get('view', 'records')
    result = save_data(tenant_id, database, table, data, case_id, case_id_based=True, view='records')
    return jsonify(result)

@app.route('/partial_match')
def partial_match_route():
    params = request.json
    input_string = params.get('input_string', None)
    matchable_strings = params.get('matchable_strings', [])
    threshold = params.get('threshold', 75)
    result = partial_match(input_string, matchable_strings, threshold=75)
    return jsonify(result)

@app.route('/date_transform')
def date_transform_route():
    params = request.json
    date = params.get('date', None)
    input_format = params.get('input_format', 'dd-mm-yyyy')
    output_format = params.get('output_format', 'dd-mm-yyyy')
    result = date_transform(date, input_format='dd-mm-yyyy', output_format='dd-mm-yyyy')
    return jsonify(result)
