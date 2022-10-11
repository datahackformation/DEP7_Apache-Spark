from pickle import TRUE
from flask import  Flask, request, jsonify
from module.database import Database

app = Flask(__name__)
db = Database()

'''
@app.route('/') #decorador
def index():
    return "hello world"
'''

@app.route("/Preguta1", methods = ['GET'])
def Preguta1():
    if request.method == 'GET' :
        try:    
            result = db.readPreguta1(request.json["nombre_dep"])
        except Exception as e:
            return e
    return jsonify(result)

@app.route("/Preguta2", methods = ['GET'])
def Preguta2():
    if request.method == 'GET' :
        try:    
            result = db.readPreguta2()
        except Exception as e:
            return e
        print(result)
    return jsonify(result)

@app.route("/Preguta3", methods = ['GET'])
def Preguta3():
    if request.method == 'GET' :
        try:    
            result = db.readPreguta3()
        except Exception as e:
            return e
        print(result)
    return jsonify(result)

@app.route("/Preguta4", methods = ['GET'])
def Preguta4():
    if request.method == 'GET' :
        try:    
            result = db.readPreguta4(request.json["id"])
        except Exception as e:
            return e
        print(result)
    return jsonify(result)


if __name__=="main":    
    app.debug = True
    app.run() #refresca automatica