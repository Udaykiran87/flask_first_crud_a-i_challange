from app import app
from config import mysql, mongodb, cassandra
from flask import jsonify
from flask import flash, request, render_template, redirect, url_for, flash
from forms import *
import csv
from datetime import datetime


def err_msg(msg):
    message = {
        'status': 404,
        'message': msg + ' ' + request.url,
    }
    respone = jsonify(message)
    respone.status_code = 404
    return respone


@app.route("/")
@app.route("/home")
def home():
    return render_template('home.html')

@app.route("/create_mysql")
def create_mysql():
    try:
        if request.method == 'GET':
            create_movies_table_query = """
            create table online_movie_rating.movies(id int auto_increment primary key,
                        title varchar(100),
                        release_year year(4),
                        genre varchar(100),
                        imdb_rating float);
            """
            conn = mysql.connection
            cursor = conn.cursor()
            cursor.execute(create_movies_table_query)
            msg = 'online_movie_rating.movies table created successfully using mySQL!. Column names are title(str),release_year(int), genre(str), imdb_rating(float)'
            flash(msg, 'message')
        else:
            return err_msg('Table can not be created using MySQL')
    except Exception as e:
        flash(e, 'error')
    finally:
        cursor.close()
        conn.close()
    return render_template('home.html')

@app.route("/insert_mysql", methods=['GET','POST'])
def insert_mysql():
    form = InsertForm()
    if form.validate_on_submit():
        try:
            if request.method == 'POST':
                title = request.form['title']
                release_year = request.form['release_year']
                genre = request.form['genre']
                imdb_rating = request.form['imdb_rating']

                insert_movies_query = """
                insert into online_movie_rating.movies ( title, release_year,genre, imdb_rating) values (%s, %s, %s, %s)
                """
                records_to_insert = (title, release_year, genre, imdb_rating)

                conn = mysql.connection
                cursor = conn.cursor()
                cursor.execute(insert_movies_query, records_to_insert)
                conn.commit()
                flash('Movie inserted into Movies table successfully', 'message')

            else:
                return err_msg('data can not be inserted')
        except Exception as e:
            flash(e, 'error')
        finally:
            cursor.close()
            conn.close()
    return render_template('insert.html', title='Insert', form=form)


@app.route("/update_mysql", methods=['GET', 'POST', 'PUT'])
def update_mysql():
    form = UpdateForm()
    if form.validate_on_submit():
        try:
            if request.method == 'POST':
                title = request.form['title']
                title = "'" + title + "'"
                column = request.form['column']
                value = request.form['value']
                if column == 'release_year':
                    value = int(value)
                elif column == 'imdb_rating':
                    value = float(value)
                else:
                    value = "'"+value+"'"

                conn = mysql.connection
                cursor = conn.cursor()
                q1 = "UPDATE online_movie_rating.movies SET {}".format(column)
                q2 = ("=%s WHERE title=%s" % (value, title))
                update_query = q1+q2
                cursor.execute(update_query)
                conn.commit()
                flash('Movies table updated successfully', 'message')
            else:
                return err_msg('table can not be updated')
        except Exception as e:
            flash(e, 'error')
        finally:
            cursor.close()
            conn.close()
    return render_template('update.html', title='Update', form=form)


@app.route("/delete_mysql", methods=['GET','POST', 'DELETE'])
def delete_mysql():
    form = DeleteForm()
    if form.validate_on_submit():
        try:
            if request.method == 'POST':
                title = request.form['title']
                title = "'" + title + "'"
                conn = mysql.connection
                cursor = conn.cursor()
                delete_query = """DELETE FROM online_movie_rating.movies WHERE title=%s;""" % title
                cursor.execute(delete_query)
                conn.commit()
                flash('Record deleted successfully', 'message')
            else:
                return err_msg('Record can not be deleted')
        except Exception as e:
            flash(e, 'error')
        finally:
            cursor.close()
            conn.close()
    return render_template('delete.html', title='delete', form=form)


@app.route("/read_download_mysql", methods=['GET', 'POST'])
def read_download_mysql():
    try:
        if request.method == 'GET':
            conn = mysql.connection
            cursor = conn.cursor()
            cursor.execute("select * from online_movie_rating.movies")
            data = cursor.fetchall()  # data from database
            flash('Table read successfully', 'message')
            return render_template("display.html", value=data, database='mysql')
        else:
            return err_msg('table can not be read')
    except Exception as e:
        flash(e, 'error')
    finally:
        cursor.close()
        conn.close()
    return render_template("display.html")


@app.route("/create_mongodb")
def create_mongodb():
    try:
        if request.method == 'GET':
            collection_name = "movies"
            collection = mongodb[collection_name]
            msg = "collection called 'movies' created successfully"
            flash(msg, 'message')
        else:
            return err_msg('collection can not be created using MongoDb')
    except Exception as e:
        flash(e, 'error')
    return render_template('home.html')


@app.route("/insert_mongodb", methods=['GET', 'POST'])
def insert_mongodb():
    form = InsertForm()
    if form.validate_on_submit():
        try:
            if request.method == 'POST':
                collection_name = "movies"
                collection = mongodb[collection_name]
                title = request.form['title']
                release_year = request.form['release_year']
                genre = request.form['genre']
                imdb_rating = request.form['imdb_rating']
                record = {'title': title,
                          'release_year': release_year,
                          'genre': genre,
                          'imdb_rating': imdb_rating
                          }
                print(record)
                collection.insert_one(record)
                flash('Movie inserted into Movies collection successfully', 'message')

            else:
                return err_msg('data can not be inserted')
        except Exception as e:
            flash(e, 'error')
    return render_template('insert.html', title='Insert', form=form)


@app.route("/update_mongodb", methods=['GET', 'POST', 'PUT'])
def update_mongodb():
    form = UpdateForm()
    if form.validate_on_submit():
        try:
            if request.method == 'POST':
                collection_name = "movies"
                collection = mongodb[collection_name]
                title = request.form['title']
                # title = "'" + title + "'"
                column = request.form['column']
                value = request.form['value']
                if column == 'release_year':
                    value = int(value)
                elif column == 'imdb_rating':
                    value = float(value)
                else:
                    value = "'"+value+"'"

                title_update = {'title':title}
                new_data = {"$set": {column: value}}
                collection.update_one(title_update, new_data)
                flash('Movies collection updated successfully', 'message')
            else:
                return err_msg('collection can not be updated')
        except Exception as e:
            flash(e, 'error')
    return render_template('update.html', title='Update', form=form)


@app.route("/delete_mongodb", methods=['GET', 'POST', 'DELETE'])
def delete_mongodb():
    form = DeleteForm()
    if form.validate_on_submit():
        try:
            if request.method == 'POST':
                collection_name = "movies"
                collection = mongodb[collection_name]
                title = request.form['title']
                # title = "'" + title + "'"
                query_to_delete = {"title": title}
                collection.delete_one(query_to_delete)
                flash('Document deleted successfully', 'message')
            else:
                return err_msg('Document can not be deleted')
        except Exception as e:
            flash(e, 'error')
    return render_template('delete.html', title='delete', form=form)


@app.route("/read_download_mongodb")
def read_download_mongodb():
    try:
        if request.method == 'GET':
            new_data = []
            collection_name = "movies"
            collection = mongodb[collection_name]
            data = collection.find()
            data = list(data)
            for i in data:
                new_data.append(tuple(i.values()))
            flash('Collection read successfully', 'message')
            return render_template("display.html", value=new_data, database='mongodb')
        else:
            return err_msg('Collection can not be read')
    except Exception as e:
        flash(e, 'error')
    return render_template("display.html")


@app.route("/create_cassandra",methods = ['GET', 'POST'])
def create_cassandra():
    try:
        if request.method == 'GET':
            session = cassandra.connect()
            create_movies_table_query = """
            create table if not exists online_movie_rating.movies2(title text primary key,
                        release_year int,
                        genre text,
                        
                        . float);
            """
            session.execute(create_movies_table_query).one()
            msg = 'online_movie_rating.movies2 table created successfully using cassendra!. Column names are title(str),release_year(int), genre(str), imdb_rating(float)'
            flash(msg, 'message')
        else:
            return err_msg('Table can not be created using Cassandra')
    except Exception as e:
        flash(e, 'error')
        print(e)
    return render_template('home.html')

@app.route("/insert_cassandra",methods = ['GET', 'POST'])
def insert_cassandra():
    form = InsertForm()
    if form.validate_on_submit():
        try:
            if request.method == 'POST':
                session = cassandra.connect()
                title = request.form['title']
                release_year = int(request.form['release_year'])
                genre = request.form['genre']
                imdb_rating = float(request.form['imdb_rating'])
                insert_movies_query = """
                insert into online_movie_rating.movies2 ( title, release_year,genre, imdb_rating) values (%s, %s, %s, %s)
                """
                records_to_insert = (title, release_year, genre, imdb_rating)
                session.execute(insert_movies_query, records_to_insert).one()
                flash('Movie inserted into Movies table successfully', 'message')
            else:
                return err_msg('data can not be inserted')
        except Exception as e:
            flash(e, 'error')
    return render_template('insert.html', title='Insert', form=form)

@app.route("/update_cassandra",methods = ['GET', 'POST','PUT'])
def update_cassandra():
    form = UpdateForm()
    if form.validate_on_submit():
        try:
            if request.method == 'POST':
                session = cassandra.connect()
                title = request.form['title']
                title = "'" + title + "'"
                column = request.form['column']
                value = request.form['value']
                if column == 'release_year':
                    value = int(value)
                elif column == 'imdb_rating':
                    value = float(value)
                else:
                    value = "'"+value+"'"

                q1 = "UPDATE online_movie_rating.movies2 SET {}".format(column)
                q2 = ("=%s WHERE title=%s" % (value, title))
                update_query = q1+q2
                session.execute(update_query)
                flash('Movies table updated successfully', 'message')
            else:
                return err_msg('table can not be updated')
        except Exception as e:
            flash(e, 'error')
    return render_template('update.html', title='Update', form=form)

@app.route("/delete_cassandra",methods = ['GET','POST', 'DELETE'])
def delete_cassandra():
    form = DeleteForm()
    if form.validate_on_submit():
        try:
            if request.method == 'POST':
                session = cassandra.connect()
                title = request.form['title']
                title = "'" + title + "'"
                conn = mysql.connection
                cursor = conn.cursor()
                delete_query = """DELETE FROM online_movie_rating.movies2 WHERE title=%s;""" % title
                session.execute(delete_query)
                flash('Record deleted successfully', 'message')
            else:
                return err_msg('Record can not be deleted')
        except Exception as e:
            flash(e, 'error')
    return render_template('delete.html', title='delete', form=form)


@app.route("/read_download_cassandra")
def read_download_cassandra():
    try:
        if request.method == 'GET':
            session = cassandra.connect()
            rows = session.execute("select * from online_movie_rating.movies2")
            _ = rows[0]
            new_data = []
            id = 0
            for row in rows:
                temp = tuple()
                id +=1
                temp = (id,row[0],row[1],row[2],row[3])
                new_data.append(temp)
            flash('Table read successfully', 'message')
            return render_template("display.html", value=new_data, database='cassandra')
        else:
            return err_msg('table can not be read')
    except Exception as e:
        flash(e, 'error')
    return render_template("display.html")


@app.route("/downloadcsv_mysql")
def downloadcsv_mysql():
    try:
        if request.method == 'GET':
            conn = mysql.connection
            cursor = conn.cursor()
            cursor.execute("select * from online_movie_rating.movies")
            data = cursor.fetchall()  # data from database
            print(data)
            print(type(data))
            now = datetime.now()
            dt_string = now.strftime("%d%m%Y%H%M%S")
            csv_name = dt_string+'.csv'
            with open(csv_name, 'w') as out:
                csv_out = csv.writer(out)
                csv_out.writerow(['id', 'title', 'release_year', 'genre', 'imdb_rating'])
                csv_out.writerows(data)
            return render_template("download.html")
        else:
            return err_msg('table can not be read')
    except Exception as e:
        print(e)


@app.route("/downloadcsv_mongodb")
def downloadcsv_mongodb():
    try:
        if request.method == 'GET':
            new_data = []
            collection_name = "movies"
            collection = mongodb[collection_name]
            data = collection.find()
            data = list(data)
            for i in data:
                new_data.append(tuple(i.values()))
            now = datetime.now()
            dt_string = now.strftime("%d%m%Y%H%M%S")
            csv_name = dt_string+'.csv'
            with open(csv_name, 'w') as out:
                csv_out = csv.writer(out)
                csv_out.writerow(['id', 'title', 'release_year', 'genre', 'imdb_rating'])
                csv_out.writerows(new_data)
            return render_template("download.html")
        else:
            return err_msg('table can not be read')
    except Exception as e:
        print(e)

@app.route("/downloadcsv_cassandra")
def downloadcsv_cassandra():
    try:
        if request.method == 'GET':
            session = cassandra.connect()
            rows = session.execute("select * from online_movie_rating.movies2")
            _ = rows[0]
            new_data = []
            id = 0
            for row in rows:
                temp = tuple()
                id +=1
                temp = (id,row[0],row[1],row[2],row[3])
                new_data.append(temp)
            now = datetime.now()
            dt_string = now.strftime("%d%m%Y%H%M%S")
            csv_name = dt_string+'.csv'
            with open(csv_name, 'w') as out:
                csv_out = csv.writer(out)
                csv_out.writerow(['id', 'title', 'release_year', 'genre', 'imdb_rating'])
                csv_out.writerows(new_data)
            return render_template("download.html")
        else:
            return err_msg('table can not be read')
    except Exception as e:
        print(e)

if __name__ == "__main__":
    app.run(debug=True)