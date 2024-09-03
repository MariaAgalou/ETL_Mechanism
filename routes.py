from flask import Blueprint, request, jsonify
from pymongo import MongoClient
import mysql.connector
from neo4jDB import Neo4J_DB
from datetime import datetime
from models import Band

# Set up MongoDB connection
client = MongoClient('mongodb://localhost:27017')
db = client['albums_db']
collection = db['albums']


# Establish a connection to Neo4j database
neo_uri = "bolt://localhost:7687"
neo_username = "neo4j"
neo_password = "123456789"

neo_conn = Neo4J_DB(uri=neo_uri, username=neo_username, password=neo_password)


# MYSQL connection setup
try:
    mysql_db = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'Playeveryday1!', 
        database = 'users_albums'
    )
    cursor = mysql_db.cursor()
    print("Successfully connected to the MySQL database")

except Exception as e:
    print(f"Error while trying to connect to MySQL: {e}")


# Create a Blueprint for routes
bands_blueprint = Blueprint('bands_blueprint', __name__)


############# MONGODB #############


# Create a new music band
@bands_blueprint.route('/bands', methods=['POST'])
def create_new_band():
    data = request.json
    try:
        band = Band(**data)
        result = collection.insert_one(band.model_dump())
        print(f"Data added: {band.model_dump()} with ID {result.inserted_id}")

        return 'Data added to MongoDB'
    except ValueError as e:
        # If data given by user are not valid, give an error as output
        print ({"ERROR" : str(e)})
        return jsonify({"ERROR" : "For more details, check output on terminal"})


# Get all music bands from the database
@bands_blueprint.route('/bands', methods=['GET'])
def get_all_bands():
    bands = collection.find()

    bands_list = list(bands)
    for band in bands_list:
        band["_id"] = str(band["_id"])

    return jsonify(bands_list)


# Get all music bands that were created in a specific time period
# URL format == http://127.0.0.1:5001/bands_by_dates?start_date=YYYY-MM-DDTHH:MM:SS&end_date=YYYY-MM-DDTHH:MM:SS
@bands_blueprint.route('/bands_by_dates', methods=["GET"])
def get_bands_by_date_range():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # Check if one or both dates are missing
    if not start_date or not end_date:
        return jsonify({"ERROR:" : "A date is missing! Please provide both start_date and end_date."})
    
    # Ensure date format is valid (ISO 8601 format)
    try:
        start_date = datetime.fromisoformat(start_date)
        end_date = datetime.fromisoformat(end_date)
    except ValueError:
        return jsonify({"ERROR:" : "Invalid date format! Dates should follow the ISO format (YYYY-MM-DDTHH:MM:SS)"})
    
    # Filter bands
    bands = collection.find({
        "date_of_formation": {
            "$gte": start_date,
            "$lte": end_date
        }
    })
    
    bands_list = list(bands)

    # Check if no music bands were found
    if not bands_list:
        return jsonify({"ERROR" : "No music bands found in the specified date range"})
    
    for band in bands_list:
        band["_id"] = str(band["_id"])

    print("Bands returned successfully")
    return jsonify(bands_list)


########## NEO4J ##########


# Helper function to serialize user's data
def serialize_user(user):
    return {
        "user_id" : user.id,
        "properties" : {
            "name": user["name"],
            "favorite_bands": user["favorite_bands"]
        }
    }


# Get a user and all his friends from Neo4J DB
@bands_blueprint.route('/users', methods=['GET'])
def get_user_and_friends():
    username = request.args.get('name')

    if not username:
        return jsonify({"ERROR" : "Provide a user name"}), 400
    
    query = """
        MATCH (user:User {name: $name})-[:FRIEND_WITH]->(friend:User)
        RETURN user, collect(friend) AS friends
    """

    result = neo_conn.query(query=query, parameters={'name' : username})

    # Check if user exists
    if not result:
        return jsonify({"ERROR": f"User with name '{username}' not found or no friends available"}), 404

    users = []
    for record in result:
        user_data = serialize_user(record["user"])
        friends_data = [serialize_user(friend) for friend in record["friends"]]
        users.append({"user": user_data, "friends": friends_data})
    
    return jsonify(users), 200

    
# Create a new user
@bands_blueprint.route('/users', methods=['POST'])
def create_new_user():
    data = request.json

    # Check if data provided by user are valid
    if not data or 'name' not in data or 'favorite_bands' not in data:
        return jsonify({"ERROR" : "Invalid input! Please provide both user's name and favorite bands."})
    
    username = data['name']
    favorite_bands = data['favorite_bands']

    # First, check if a user with this name already exists in the database
    query = """
        MATCH (user:User {name: $name})
        RETURN user
    """
    # If so, return error message
    if neo_conn.query(query=query, parameters={'name': username}):
        return jsonify({"ERROR": f"User with name '{username}' already exists."})
    
    # Create new user
    second_query = """
        CREATE (user:User {name: $name, favorite_bands: $favorite_bands})
        RETURN user
    """

    result = neo_conn.query(query=second_query, parameters={'name' : username, 'favorite_bands' : favorite_bands})
    if result:
        return jsonify({"DONE" : "User created successfully"})
    else:
        return jsonify({"ERROR" : "Failed to create user"})
    

# Create a friendship between two users
@bands_blueprint.route('/friendships', methods=["POST"])
def create_friendship():
    data = request.json

    # Check if data are valid
    if not data or 'user1' not in data or 'user2' not in data:
        return jsonify({"ERROR" : "Invalid input! Please provide the name of both users."})

    username1 = data['user1']
    username2 = data['user2']

    # First, check if one or both of the users doesn't exist in the database
    query = """
        MATCH (user1:User {name: $user1}), (user2:User {name: $user2})
        RETURN user1, user2
    """
    
    result = neo_conn.query(query=query, parameters={'user1': username1, 'user2': username2})
    if not result:
        return jsonify({"ERROR": "One or both users not found in the database. Please ensure that both users exist before creating a friendship between them."})

    # Create the friendship relationship between the two users
    second_query = """
        MATCH (u1:User {name: $user1}), (u2:User {name: $user2})
        MERGE (u1)-[:FRIEND_WITH]->(u2)
        MERGE (u2)-[:FRIEND_WITH]->(u1)
        RETURN u1, u2
    """

    output = neo_conn.query(query=second_query, parameters={'user1': username1, 'user2': username2})
    if output:
        return jsonify({"DONE" : f"Friendship was created between {username1} and {username2}."})
    else:
        return jsonify({"ERROR" : "Failed to create friendship."})


############# MYSQL #############


# Integrate data fetched from the two Kafka topics & save processed data to MYSQL database
@bands_blueprint.route('/insert_to_mysql', methods=['POST'])
def insert_to_mysql():
    """
        Function that processes the messages received from the Kafka consumer, 
        extracts relevant information and inserts this info into a MySQL table.
    """
    msg = request.json

    if msg['topic'] == 'bands-topic':
        print("BANDS-TOPIC\n")

        band_data = msg['value']
        print("Received band data:", band_data)

        # Get data about the music band
        band_id = band_data.get('_id')
        print("\nID:", band_id)
        band_name = band_data.get('name')
        print("\nName:", band_name)
        date_of_formation = band_data.get('date_of_formation')
        print("\nDate:", date_of_formation)
        band_members = band_data.get('members')
        print("\nMembers:", band_members)
    
        # Query to insert band's data into bands table
        insert_band_query = """
            INSERT INTO bands (band_id, band_name, date_of_formation, band_members)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            band_name = VALUES(band_name), date_of_formation = VALUES(date_of_formation), band_members = VALUES(band_members);
        """

        # Execute query and commit to database
        cursor.execute(insert_band_query, (band_id, band_name, date_of_formation, band_members))
        mysql_db.commit()

        # Insert albums of the band into the albums table
        albums = band_data.get('albums', [])
        for album in albums:
            insert_album_query = """
                INSERT INTO albums (band_id, album_title)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                album_title = VALUES(album_title);
            """

            cursor.execute(insert_album_query, (band_id, album))
            mysql_db.commit()


    elif msg['topic'] == 'users-topic':
        print("USERS-TOPIC\n")

        user_data = msg['value']['user']
        print(user_data)

        # Get data about the user
        user_id = user_data['user_id']
        print("\nID:", user_id)
        username = user_data['properties'].get('name')
        print("\nUsername:", username)
        favorite_bands = user_data['properties'].get('favorite_bands', [])
        print("\nFav bands:", favorite_bands)

        # Query to insert user's data into users table
        insert_user_query = """
            INSERT INTO users (user_id, username)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
            username = VALUES(username);
        """
        # Execute query and commit to database
        cursor.execute(insert_user_query, (user_id, username))
        mysql_db.commit()

        # Insert user's favorites bands into the user_favorites table
        for band_name in favorite_bands:
            print("current band is: ", band_name)
            #cursor.execute("SELECT band_id FROM bands WHERE band_name = %s", (band_name,))
            cursor.execute("SELECT band_id FROM bands WHERE LOWER(band_name) = LOWER(%s)", (band_name.strip(),))
            result  = cursor.fetchone()

            if result:
                band_id = result[0]
                print(band_id)

                insert_favorites_query = """
                    INSERT INTO user_favorites (user_id, band_id)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                    band_id = VALUES(band_id);
                """
                cursor.execute(insert_favorites_query, (user_id, band_id))
                mysql_db.commit()

    return jsonify({"STATUS" : "Data inserted successfully into MySQL Database."}), 201

