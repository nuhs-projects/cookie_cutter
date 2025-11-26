
# Readme


# Flow

![flow](./docs/pics/flow.png)

#### Final message to kafka:

```
{
  "result_prediction": {
    "predictor": "agilos",
    "result": "1",
    "mrn": "agilos0",
    "alert_tier": 3,
    "alert_trigger": 3,
    "metadata": "{\"consumerInfo\": \"Topic: nuhs.eai.hl7.mdm | Partition: 0 | Offset: 2604 | Key: None\", \"text\": [\"hist\", \"exam\", \"note\", \":\", \"he\", \"correct\", \"<newline>\"], \"attention\": [0.7135450301688524, 1.0, 0.7002395978140147, 0.6848392629042026, 0.7277016507670317, 0.07887794946196224, 0.20815984097913312]}"
  }
}

```

### Credits

- Qinfeng

- Service team




#### Things to note

- **IMPORTANT**: A decent understanding of tools such as Databases, Kafka, Docker, K8, etc.

- Nothing special about this repo, you probably could better job at it

- So what's the point of this repo?
    - You have a starter reference project instead of staring at a blank canvas

#### Setup 

```
pip install -r requirements.txt
```


##### Code Structure

```
.
├── deploy
│   ├── docker (Docker config files)
│   └── k8  (k8s config files)
├── nuhs_lst (don't modify this if you want to use the same flow)
│   ├── config
│   ├── db_manager.py (database manager for db queries & insertions)
│   ├── kafka
│   │   ├── consumer.py 
│   │   ├── kafka_manager.py (manages kafka consumer + producer)
│   │   └── producer.py
│   ├── striker.py (something like a main function)
│   ├── utils (utility scripts)
│   └── web.py
├── requirements.txt (library requirements file)
├── run.py
├── user_dir (put your model, scripts, test scripts, requirements and anything you want)
│   ├── agimodel.py
│   ├── best_model
│   │   └── pytorch.bin
│   ├── lst_model.py
│   ├── reference
│   │   └── bed_class_type_mapping.csv
│   ├── requirements.txt (user's requirements file)
```

---

####  What if I want to just to the change data & model but keep the same flow?
Modify the following:

- deploy/ (change docker & k8 files)

- implement a lst_model.py with LSTModel + relevant methods (refer below) within user_dir

---

##### LSTModel Class Documentation

Overview

The `LSTModel class` is responsible for processing kafka messages, enriching them with additional data, and making predictions using a trained model.

Functions

1. `__init__(self, projectName)`

    Description: Initializes the LSTModel instance by loading model weights, reading mappings, and setting up required attributes. 

    Arguments:  
    > projectName (str): The name of the project


2. `process_msg(self, msg)`

    Description: Processes an incoming kafka message, extracts relevant fields, and validates whether it should be further processed for prediction. 

    Arguments:  
    > msg (str): Usually a JSON-formatted string containing clinical message data. 

    Returns:  
    > dict: Processed data if valid; None if the message is ignored.

3. `enrich(self, dbm, data)`

    Description: Enhances the processed message with additional data retrieved from a database.

    Arguments:  
    > dbm (Database Manager): A database connection object used to fetch patient data.  
        data (dict): The processed message data.  

    Returns:  
    > dict: The enriched data.

4. `predict(self, data, metadata={})`

    Description: Runs the prediction model on the enriched data and returns the prediction results.

    Arguments:  
    > data (dict): The enriched message data to be evaluated.  
        metadata (dict, optional): Additional metadata to be stored in db (no restriction).  

    Returns:  
    > dict: The prediction results which also contains the metadata

---

### What if I want to do further customizations (e.g. produce to two topics, etc.)?

You may refer to the following files

- kafka_manager.py 

- consumer.py 