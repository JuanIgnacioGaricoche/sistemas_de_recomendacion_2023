Explicacion del repo:
La estructura de archivos es similar a la de la materia.
En el directorio principal:
    * alison_flask_app.py es el archivo principal a ejecutar.
    * Desde alison_flask_app.py se llama a alison_recomendar.py y carga los htmls ubicados en templates.
    * En hyper_tuning.py se hace un grid search cv para buscar los mejores hiperparametros del lightfm.
En la carpeta data se encuentra:
    * La base de datos alison_data.db
    * Tres archivos .csv con los datos scrapeados de alison.com
    * El script que corri para scrapear los datos scrapping.py (un poco desordenado)
    * El script que use para crear la base de datos data_wrangling.py
    * Dos archivos pickle, resultados de la optimizacion de hiperparametros del lightfm

La idea del sitio es:
    * Recomendar cursos gratuitos online, presentes en alison.com
    * Si el usuario no presenta interacciones, recomendar los 9 mas populares.
    * Para calcularlos, se tiene en cuenta un score construido en base a las valoraciones (de 1 a 5) y la cantidad de interacciones.
    * Si el usuario tiene entre 6 y 10 interacciones, recomendar los cursos basados en las interacciones.
    * Para esto, se identifica la categoria y el ambiente de los cursos, y se recomiendan cursos similares a los que el usuario puntuo positivamente.
    * Notar que si, por ejemplo, el usuario puntua con 5 puntos los cursos de una misma categoria, entonces este recomendador va a sugerir cursos de esa categoria.
    * Si el usuario interactuo con mas de 10 cursos, entonces se recomiendan los cursos basados en lightfm. Se corrio una optimizacion para definir el modelo con mejor precision a las 9 recomendaciones.
    * En ningun momento se deberian recomendar cursos con los que el usuario ya interactuo (valoracion entre 1 y 5).

