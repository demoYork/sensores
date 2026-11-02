import pymongo
from transformers import pipeline
import time
import mysql.connector

# Initialize the Hugging Face text classifier
# The 'distilbert-base-uncased' model is lightweight and fast
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

# Define the classification labels for temperature and light level
labels1 = ["high", "medium", "low"]
labels2 = ["Very bright", "bright", "not very bright", "dim", "dull"]


# Callback to handle incoming MQTT messages
def predict_temperature(temperature):
    try:
        # Text to classify the temperature level
        text = f"Given that a temperature value can range from 0 to 100 degrees Celsius, how would you classify a Celsius value {temperature}: high, medium, or low?"

        # Call the Hugging Face classifier to classify the temperature
        classification = classifier(text, labels1)

        return classification
    except Exception as e:
        print(f"Error procesando el mensaje: {e}")


# Callback para manejar los mensajes MQTT entrantes
def predict_light(light):
    try:
        # Texto para clasificar
        # Usamos una pregunta que el modelo pueda clasificar
        text = f"How bright is the value {light} considered, Very bright, bright, not very bright, dim, dull, the light range is between 1 and 6000."

        # Clasificar la temperatura usando el modelo de Hugging Face
        classification = classifier(text, labels2)

        return classification
    except Exception as e:
        print(f"Error procesando el mensaje: {e}")


# ----------------------------------------------------------------------------------------------
# MySQL configuration
# This configuration connects to a MySQL database running on localhost at port 3306.
# It uses the 'sentimientos' database and the 'mensajes' table.
# The 'mensajes' table has the following columns: doc_id, user_id, message, sentiment.
# ----------------------------------------------------------------------------------------------
mydb = mysql.connector.connect(host="127.0.0.1", port="3306", user="root", passwd="parda99*", database="sensores")
mycursor = mydb.cursor()


# Store the processed documents into MySQL database
def insert_into_mysql(doc_id, device_id, ll, cllight, lt, cltemp):
    sql = "INSERT INTO esp32(doc_id, device_id, light_lvl, class_light, temperature, class_temp) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (doc_id, device_id, ll, cllight, lt, cltemp)
    print(val)
    mycursor.execute(sql, val)
    mydb.commit()


# ----------------------------------------------------------------------------------------------
# MongoDB configuration
# This configuration connects to a MongoDB instance running on localhost at port 27017.
# ----------------------------------------------------------------------------------------------
conex = pymongo.MongoClient(host=['127.0.0.1:27017'], username='admin', password='parda99*')
db = conex.esp32_data


# This function retrieves documents with status "received", processes them, and updates their status to "processed".
# It also includes a delay of 5 seconds to avoid overwhelming the database with requests.
def process_documents():
    docus = db.sensores.find({"status": {"$eq": "received"}}).limit(5)
    for doc in docus:
        # Predict the temperature
        temperature = predict_temperature(doc['temperature_level'])
        light = predict_light(doc['light_level'])

        print(f"Predicted temperature: {temperature}")

        # call the function to insert the document into MySQL
        insert_into_mysql(str(doc["_id"]), doc["device_id"], int(doc["light_level"]), light['labels'][0], float(doc["temperature_level"]),
                          temperature['labels'][0])

        db.sensores.update_one(
            {"_id": doc["_id"]},
            {"$set": {"status": "processed"}}
        )

    # Apply a delay of the 5 seconds to avoid overwhelming the database with requests
    time.sleep(5)


# Main loop to continuously process documents
while True:
    try:
        process_documents()
    except Exception as e:
        print(f"An error occurred: {e}")
        time.sleep(5)  # Wait before retrying in case of an error
    except KeyboardInterrupt:
        print("Process interrupted by user.")
        break
