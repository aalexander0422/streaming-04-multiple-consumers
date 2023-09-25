"""
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.
    Ashlee A 9/24
   
"""

import pika
import sys
import webbrowser
import csv
import time


# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

SHOW_OFFER = False

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    logger.info("Seeing if you want to monitor RabbitMQ queues")
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logger.info(f"Answer is {ans}.")

def send_message(host: str, queue_name: str, input_file: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        
        # Read the tasks.csv file and send each task to the queue
        with open(input_file, 'r') as input_file:
            reader = csv.reader(input_file)
            for row in reader:
                message = str(row)
                # use the channel to publish a message to the queue
                # every message passes through an exchange
                ch.basic_publish(exchange="", routing_key=queue_name, body=message)
                # print a message to the console for the user
                logger.info(f" [x] Sent {message}")
                # Wait 2 seconds between each message
                time.sleep(2)
                
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # See if offer_rabbitmq_admin_site() should be called
    if SHOW_OFFER == True:
        # ask the user if they'd like to open the RabbitMQ Admin site
        offer_rabbitmq_admin_site()
    # send the message to the queue
    send_message("localhost","task_queue3","tasks.csv")