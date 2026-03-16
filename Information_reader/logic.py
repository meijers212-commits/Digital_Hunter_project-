import json
import math


class Logic:

    @staticmethod
    def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate the great-circle distance in km between two points on Earth."""
        EARTH_RADIUS_KM = 6371.0

        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)

        a = (
            math.sin(delta_lat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return EARTH_RADIUS_KM * c

    @staticmethod
    def process_message(validate_schema, save_to_db, log_event, db_connection, db_name,  producer, msg_value, topic):
        
        try:
            data = json.loads(msg_value.decode('utf-8'))
        except json.JSONDecodeError:
            log_event(level="error",message="Invalid json, sending kafka")
            producer.publish(msg_value)
            return

        
        if not validate_schema(data, topic):
            log_event(level="warning",message=f"logical error in {topic}, sending")
            producer.publish(msg_value)
            return


        save_to_db(
            log_event=log_event,
            db_connection=db_connection,
            db_name=db_name,
            data=data,
            topic=topic
            )
        
        log_event(level="info",message=f"message processed successfully from {topic}")

    @staticmethod
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


    @staticmethod
    def save_to_db(log_event, db_connection, db_name, data, topic):

        cursor = db_connection.cursor()
        cursor.execute(f"USE {db_name}")

        data["distance"] = 0

        try:
            if topic == "intel":
                    entity_id = data['entity_id']
                    
                    cursor.execute("SELECT lat, lon, priority_level FROM target_bank WHERE entity_id = %s", (entity_id,))
                    result = cursor.fetchone()

                    if result:
                        lat, lon, priority_level = result
                        distance = Logic.haversine_km(lat, lon, data['reported_lat'], data['reported_lon'])
                        
                        
                        query = "UPDATE target_bank SET lat=%s, lon=%s, distance=%s WHERE entity_id=%s"
                        cursor.execute(query, (data['reported_lat'], data['reported_lon'], distance, entity_id))
                    
                    else:
                        query = "INSERT INTO target_bank (entity_id, lat, lon, priority_level, status) VALUES (%s, %s, %s, %s, %s)"
                        cursor.execute(query, (entity_id, data['reported_lat'], data['reported_lon'], 99, 'active'))
            
            elif topic == "attack":
                query = "UPDATE target_bank SET last_weapon = %s WHERE entity_id = %s"
                cursor.execute(query, (data['weapon_type'], data['entity_id']))
                
            elif topic == "damage":
                query = "UPDATE target_bank SET status = %s WHERE entity_id = %s"
                cursor.execute(query, (data['result'], data['entity_id']))
            
            db_connection.commit()
            log_event(level="info", message=f"Successfully processed {topic} for {data.get('entity_id')}")
            
        except Exception as e:
            log_event(level="error", message=f"db save failed for {topic}: {e}")
        finally:
            cursor.close() 


