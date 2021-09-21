from app import app
from flask_mysql_connector import MySQL
from flask_pymongo import PyMongo
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'mysql'
app.config['MYSQL_DATABASE'] = 'online_movie_rating'
app.config['MYSQL_HOST'] = 'localhost'
app.config['SECRET_KEY'] = 'you-will-never-guess'
mysql = MySQL(app)

app.config["MONGO_URI"] = "mongodb://localhost:27017/online_movie_rating"
mongodb_client = PyMongo(app)
mongodb = mongodb_client.db



cloud_config= {
        'secure_connect_bundle': 'C:\\iNeuronClass\\python\\22052021\\secure-connect-test2.zip'
}
auth_provider = PlainTextAuthProvider('OgIPRCakZCmxWlhnciKruaqR', 'r2NRv1vZIwoyfjH2idXSRwWZNZnSpwpxE1khQWAQMTj20OtpMj0tC0JMozEY7_n2_XWroSoksG-hiU1Mw5zAnZokcP4j+KGE7a7Kh-igHAAWiAQUiWPqCfQC_,h5gL9c')
cassandra = Cluster(cloud=cloud_config, auth_provider=auth_provider)

