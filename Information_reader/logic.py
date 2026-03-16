import json
import mysql.connector

def process_message(save_to_db, log_event, db_connection,  producer, msg_value, topic):
    
    try:
        data = json.loads(msg_value.decode('utf-8'))
    except json.JSONDecodeError:
        log_event(level="error",message="Invalid JSON, sending to DLQ")
        producer.publish(msg_value)
        return

    
    if not validate_schema(data, topic):
        log_event(level="warning",message=f"logical error in {topic}, sending")
        producer.publish(msg_value)
        return

    save_to_db(
        log_event=log_event,
        db_connection=db_connection,
        data=data,
        topic=topic
        )
    log_event(level="info",message=f"Message processed successfully from {topic}")


def validate_schema(data, topic):
  
    required_fields = {
        "intel": ["entity_id", "reported_lat", "reported_lon"],
        "attack": ["attack_id", "entity_id", "weapon_type"],
        "damage": ["attack_id", "result"]
    }
    
 
    if not all(field in data for field in required_fields.get(topic, [])):
        return False
        
   
    if topic == "damage" and data.get("result") not in ["destroyed", "damaged", "no_damage"]:
        return False
        
    return True



def Get_sql_db_connection(host,user,password,database):
    db_conn = mysql.connector.connect(
        host="mysql-db", 
        user="root",
        password="password",
        database="hunter_db"
    )
    return db_conn

def save_to_db(log_event, db_connection, data, topic):

    cursor = db_connection.cursor()

    try:
        if topic == "intel":
            query = "INSERT INTO intel (entity_id, lat, lon) VALUES (%s, %s, %s)"
            cursor.execute(query, (data['entity_id'], data['reported_lat'], data['reported_lon']))
        
        elif topic == "attack":
            query = "INSERT INTO attacks (attack_id, entity_id, weapon) VALUES (%s, %s, %s)"
            cursor.execute(query, (data['attack_id'], data['entity_id'], data['weapon_type']))
            
        elif topic == "damage":
            query = "UPDATE attacks SET result = %s WHERE attack_id = %s"
            cursor.execute(query, (data['result'], data['attack_id']))
        
        cursor.commit()
        db_connection.commit() 
    except Exception as e:
        log_event(level="error", message=f"db save failed: {e}")


                  