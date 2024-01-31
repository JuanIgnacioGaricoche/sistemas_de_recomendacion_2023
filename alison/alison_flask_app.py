from flask import Flask, request, render_template, make_response, redirect
#import recomendar
import alison_recomendar
import sys

app = Flask(__name__)

@app.route('/', methods=('GET', 'POST'))
def login():
    # si me mandaron el formulario y tiene user_id... 
    if request.method == 'POST' and 'user_id' in request.form:
        lastname = request.form['user_id']
        # creo el usuario al insertar un registro en profile
        alison_recomendar.crear_usuario(lastname)
        
        
        user_id = alison_recomendar.consultar_user_id(lastname)

        # mando al usuario a la página de recomendaciones
        res = make_response(redirect("/recomendaciones"))

        # pongo el user_id en una cookie para recordarlo
        res.set_cookie('user_id', str(user_id))
        return res

    # # si alguien entra a la página principal y conozco el usuario
    # if request.method == 'GET' and 'user_id' in request.cookies:
    #     return make_response(redirect("/recomendaciones"))

    # sino, le muestro el formulario de login
    return render_template('alison_login.html')

@app.route('/recomendaciones', methods=('GET', 'POST'))
def recomendaciones():
    user_id = request.cookies.get('user_id')

    # me envían el formulario
    if request.method == 'POST':
        for course_id in request.form.keys():
            rating = int(request.form[course_id])
            alison_recomendar.insertar_interacciones(course_id, user_id, rating)

    # recomendaciones
    cursos = alison_recomendar.recomendar(user_id)

    # pongo libros vistos con rating = 0
    for curso in cursos:
        alison_recomendar.insertar_interacciones(curso["course_id"], user_id, 0)

    cant_valorados = len(alison_recomendar.valorados(user_id))
    cant_ignorados = len(alison_recomendar.ignorados(user_id))
    
    return render_template("alison_recomendaciones.html", cursos=cursos, user_id=user_id, cant_valorados=cant_valorados, cant_ignorados=cant_ignorados)

@app.route('/reset')
def reset():
    user_id = request.cookies.get('user_id')
    alison_recomendar.reset_usuario(user_id)

    return make_response(redirect("/recomendaciones"))


if __name__ == "__main__":
    app.run(debug=True)