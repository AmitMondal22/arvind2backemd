
from db_model.MqttData import mqtt_topic_name
async def update_topics():
    # Retrieve topics from the database
    data = await mqtt_topic_name()

    # Generate EMS topic names
    # ems_topics = [("/water_ms/" + data[i]['concatenated_string'], 0) for i in range(len(data))]
    # ems_topics += [("/settings/" + data[i]['concatenated_string'], 0) for i in range(len(data))]
    # ems_topics.append(('/registration/AA', 0))
    
    # ems_topics = [("/ARVIND/" + data[i]['concatenated_string'], 0) for i in range(len(data))]
    # ems_topics += [("/ARVIND_settings/" + data[i]['concatenated_string'], 0) for i in range(len(data))]
    
    
    ems_topics = [("/ARVIND/#", 0) for i in range(len([1]))]
    ems_topics += [("/ARVIND_settings/#", 0) for i in range(len([1]))]
    
    

    print("\n================================",ems_topics)

    # Combine EMS and UPSMS topic lists
    all_topics = ems_topics 

    return all_topics