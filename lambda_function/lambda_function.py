import os 
import boto3
import json
import logging
import re
from pathlib import Path
from decimal import Decimal, getcontext
from typing import Union

"""
This function is to process MQTT data payload from vehicle logger via AWS IoT
Core and write to corresponding AWS timestream table.

1. On receiving MQTT message, IoT rule triggers lambda function
2. Initialize logging function
3. Validate message contains data payload, clientID, MQTT topic
4. Parse message into data payload (dict) and metadata (dict)
5. Retrieve thingAttributes from thing registry using metadata['device_id]
6. Process data payload for database ingestion
7. Verify VIN (if applicable) is in data payload is the same as the one in thingAttributes
8. Compose record and write for AWS timestream

"""
log = logging.getLogger()

def lambda_handler(event: dict, context)->None:
    init()
    validate_event(event)

    log.info(f"Received payload: {json.dumps(event)}")
    log.info(f"Function name: {context.function_name}")
    log.info(f"AWS request ID: {context.aws_request_id}")
    log.info(f"Remaining time: {context.get_remaining_time_in_millis()} milliseconds")

    clientid = event["clientid"]
    log.debug(f"MQTT client ID: {clientid}")
    topic = event["topic"]
    log.debug(f"MQTT topic: {topic}")
    
    try:
        metadata = get_metadata(topic=topic)
        payload = get_payload(event=event)
        thing_attributes = get_thingAttributes(metadata=metadata)
        transformed_payload = transform_payload(payload=payload, thing_attributes=thing_attributes)

        timestream_record = create_ts_record(thing_attributes=thing_attributes, transformed_payload=transformed_payload)
        result = timestream_write(thing_attributes=thing_attributes, timestream_record=timestream_record)

        return {"statusCode": 200, "body": {"TS_W_Result": result}}
    except Exception as err:
        log.error(err)
        raise

# Initialize lambda function
def init() -> None:
    log.setLevel(logging.DEBUG)

# Validate message contains required objects for further processing
def validate_event(event: dict) -> None:
    if "clientid" not in event:
        raise Exception("'clientid' is required.")
    if "topic" not in event:
        raise Exception("'topic' is required.")
    if not (
        event["topic"].startswith("data/") and len(event["topic"].split("/")) == 4
    ):
        raise Exception("MQTT topic does not match naming convention")
    log.info("Event validated")


# Retrieve message metadata from topic ('data/<device_model>/<organization>/<device_id>)
def get_metadata(topic: str) -> dict:
    metadata_keys = ["device_model", "organization", "device_id"]
    metadata = {key: value for key, value in zip(metadata_keys, topic.split("/")[2::])}

    log.debug(f"Parsed message metadata: {metadata}")
    return metadata


# Retrieve message payload
def get_payload(event: dict) -> dict:
    payload = {
        key: value for key, value in event.items() if key not in ("topic", "clientid")
    }
    log.debug(f"Message payload: {payload}")
    return payload


# Retrieve thingAttributes from thing registry
def get_thingAttributes(metadata: dict) -> dict:
    try:
        log.debug(f"Retrieving thingAttributes of {metadata['device_id']}")
        iot_client = boto3.client(
            service_name="iot", region_name=os.environ['AWS_REGION']
        )  # Arguments to be environment variables
        response = iot_client.describe_thing(thingName=metadata["device_id"])

        log.debug(f"Retrieved response: {response}")
        thingAttributes = response["attributes"]
        log.debug(f"Rectrieved thingAttributes: {thingAttributes}")
        return thingAttributes

    except Exception as err:
        log.error(f"Fail to retrieve thingAttributes: {str(err)}")
        raise


# Test decimal value type
def test_decimal(value: Union[int,float]):
    try:
        float_value = float(value)
        int_value = int(float_value)
        if float_value == int_value:
            return int_value
        else:
            return float_value
    except ValueError:
        return None

# Process special key for timestream record generation
def process_special_keys(transformed_payload:dict)->dict:
    log.debug("Discovering special keys")
    special_keys = ["Latitude and Longitude"]
    for special_key in special_keys:    
        if special_key in transformed_payload:
            log.debug(f"Discovered special key: {special_key}")
            if special_key == "Latitude and Longitude":
                key_splitter = " and "
                value_splitter = ','
            # Add new special keys check here if applicable
                
            try:
                keys = special_key.split(key_splitter)
                values = transformed_payload[special_key].split(value_splitter)
                for key, value in zip(keys, values):
                    transformed_payload[key] = float(value)
                del transformed_payload[special_key]
                log.debug(f"Process {special_key} complete")
            except Exception as err:
                log.error(f"Error processing special key: {err}")
                raise
        else:
            log.debug("No special key discovered")
    return transformed_payload


# Transform payload 
def transform_payload(payload:dict, thing_attributes:dict)->dict:
    param_lib = Path(__file__).resolve().parent/'params'/f"{thing_attributes['device_model']}.json"
    log.debug(f"Parameter library path: {param_lib}")

    try:
        with open(file=param_lib, mode="r") as params:
            params = json.load(params)
            log.debug(f"Retrieved parameter library: {params}")
    except Exception as err:
        log.error(f"Unable to find parameter library: {err}")
        raise

    transformed_payload = {}
    try:
        # Transform payload with parameter library
        for item in params:
            property_id = item["Property ID"]
            property_name = item["Property Name"]
            multiplier = item["Multiplier"]
            
            if property_id in payload:
                value = payload[property_id]
                log.debug(f"Matched parameter: property_id={property_id} | property_name={property_name} | multipier={multiplier} | value={value}")

                if (isinstance(value, str) and re.match(r"^0x[0-9A-Fa-f]+$", value)):
                    log.debug(f"Detected hexadecimal value: {value}")
                    value = int(value, 16)
                    log.debug(f"Transformed to decimal value: {value}")

                if isinstance(value, (int, float)) and multiplier is not None:
                    multiplier = test_decimal(multiplier)
                    if multiplier is not None:
                        if (isinstance(multiplier, int) and isinstance(value, int)):
                            log.debug("Applying multiplier to value")
                            log.debug(f"int multiplier: {multiplier} | int value: {value}")
                            value *= multiplier
                            log.debug(f"Product int: {value}")
                        elif (isinstance(multiplier, float) or isinstance(value, float)):
                            log.debug("Applying multiplier to value")
                            log.debug(f"float multiplier: {multiplier} | float value: {value}")
                            getcontext().prec = 10
                            value = float(Decimal(value)*Decimal(multiplier))
                            log.debug(f"Product float: {value}")

                transformed_payload[property_name] = value
            
        # Process special keys in transformed payload
        transformed_payload = process_special_keys(transformed_payload)

    except Exception as err:
        log.error(f"Unable to process payload: {err}")
        raise

    log.info(f"Transformed payload: {transformed_payload}")    
    return transformed_payload

# Process and create timestream record
def create_ts_record(thing_attributes:dict, transformed_payload:dict)-> dict:
    try:
        if 'VIN' not in transformed_payload:
            raise Exception("VIN not included in message payload")
        if thing_attributes['vehicle_id'] != transformed_payload['VIN']:
            raise Exception(f"VIN: {transformed_payload['VIN']} from message payload does not match vehicle_id:{thing_attributes['vehicle_id']} in thing registry")
        log.info(f"Verified VIN:{transformed_payload['VIN']} from message payload matches vehicle_id: {thing_attributes['vehicle_id']}")
        del transformed_payload['VIN']

        timestamp = str(transformed_payload['Timestamp'])
        if len(timestamp) == 10:
            time_unit = 'SECONDS'
        elif len(timestamp) == 13:
            time_unit = 'MILLISECONDS'
        elif len(timestamp) == 16:
            time_unit = 'MICROSECONDS'
        elif len(timestamp) == 19:
            time_unit == 'NANOSECONDS'
        else:
            raise Exception("Timestamp unit not supported")
        log.info(f"Verified timestamp: {transformed_payload['Timestamp']}")
        del transformed_payload['Timestamp']
        
        dimensions = [{'Name':key, 'Value': value} for key,value in thing_attributes.items()]

        common_attributes = {
            'Time': timestamp,
            'TimeUnit': time_unit,
            'Dimensions': dimensions
        }

        records = [
            {
                'MeasureValueType': "DOUBLE" if isinstance(value, float) else
                                    "BIGINT" if isinstance(value, int) else
                                    "VARCHAR" if isinstance(value, str) else
                                    "BOOLEAN" if isinstance(value, bool) else
                                    None,  # Default value for unsupported types
                'MeasureName': key,
                'MeasureValue': str(value)
            }
            for key, value in transformed_payload.items()
        ]
        # Check if any value type is None
        if any(record['MeasureValueType'] is None for record in records):
            raise Exception("One or more values have an unsupported type")
        
        timestream_record = {
            'Common_Attributes': common_attributes,
            'Records': records
        }

        log.info(f"Generated timestream record: {timestream_record}")
        return timestream_record

    except Exception as err:
        log.error(f"Error creating record: {err}")
        raise


# Compose record and write to timestream table
def timestream_write(thing_attributes:dict,timestream_record: dict)->dict:
    try:
        ts_client = boto3.client(service_name="timestream-write", region_name=os.environ['AWS_REGION'])
        result = ts_client.write_records(
            DatabaseName=thing_attributes['organization'],
            TableName=thing_attributes['vehicle_id'],
            Records=timestream_record['Records'],
            CommonAttributes=timestream_record['Common_Attributes']
            )
        log.info(result)
        return result

    except ts_client.exceptions.RejectedRecordsException as err:
        log.error(f"RejectedRecords: {err}")
        for rr in err.response['RejectedRecords']:
            log.error(f"Rejected index {str(rr['RecordIndex'])}: {rr['Reason']}")
            if 'ExistingVersion' in rr:
                log.error(f"Rejected record existing version: {rr['ExistingVersion']}")
        raise

    except Exception as err:
        log.error(f"{err}")
        raise
