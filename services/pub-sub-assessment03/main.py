import functions_framework  # Imports the Functions Framework for Google Cloud Functions
from flask import jsonify, Request  # Imports jsonify for JSON responses and Request for HTTP request handling
from google.cloud import pubsub_v1  # Imports the Pub/Sub client library
import json  # For encoding and decoding JSON data
import logging  # For logging errors and information

publisher = pubsub_v1.PublisherClient()  # Creates a Pub/Sub publisher client
topic_path = publisher.topic_path("playground-s-11-2d8e1903", "sneha-topic")  # Constructs the full topic path

processed_requests = set()  # Initializes a set to keep track of processed request IDs (for idempotency)

@functions_framework.http  # Decorator to define an HTTP-triggered Cloud Function
def publish_message(request: Request):  # Function to handle incoming HTTP requests
    try:
        data = request.get_json(force=True)  # Parses the request body as JSON, even if the content-type is not set
        if not data or "message" not in data or "request_id" not in data:  # Validates required fields
            return jsonify({"error": "Invalid payload"}), 400  # Returns 400 if validation fails

        request_id = data["request_id"]  # Extracts the request ID from the payload
        message = data["message"]  # Extracts the message from the payload

        if request_id in processed_requests:  # Checks if this request has already been processed
            return jsonify({"message": "Already processed"}), 202  # Returns 202 if already processed

        attributes = {  # Prepares message attributes for Pub/Sub
            "request_id": request_id,
            "source": "http-function"
        }

        future = publisher.publish(  # Publishes the message to Pub/Sub asynchronously
            topic_path,
            json.dumps(message).encode("utf-8"),  # Serializes the message to JSON and encodes as bytes
            **attributes  # Adds attributes to the Pub/Sub message
        )
        message_id = future.result()  # Waits for the publish to complete and gets the message ID
        processed_requests.add(request_id)  # Adds the request ID to the set of processed requests

        return jsonify({"messageId": message_id}), 202  # Returns the Pub/Sub message ID with 202 Accepted

    except Exception as e:  # Handles any exceptions during processing
        logging.exception("Error publishing message")  # Logs the exception with traceback
        return jsonify({"error": str(e)}), 500  # Returns 500 Internal Server Error with error details
