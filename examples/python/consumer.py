import grpc
import time

import broker_pb2
import broker_pb2_grpc

def run():
    print("Starting Python consumer...")

    keepalive_options = [
        ('grpc.keepalive_time_ms', 10000),             
        ('grpc.keepalive_timeout_ms', 5000),           
        ('grpc.keepalive_permit_without_calls', True), 
        ('grpc.http2.max_pings_without_data', 0)       
    ]

    with grpc.insecure_channel('localhost:8080',options=keepalive_options) as channel:

        try:
            stub = broker_pb2_grpc.ConsumerServiceStub(channel)
        except AttributeError:
            print("ERROR: Check the name of the Stub in broker_pb2_grpc.py")
            return

        channel_to_listen = "example-channel"
        request = broker_pb2.PullRequest(
            channel=channel_to_listen,
            all_messages = True # If you dont want to get old messages that are stored set to False
        
        )

        print(f"Subscribed to the channel: '{channel_to_listen}'. Waiting messages...\n")

        try:
            stream_messages = stub.Pull(request)

            for messages in stream_messages:
                received_text = messages.payload.decode('utf-8')
                print(f"[NEW MESSAGE] -> {received_text}")

        except grpc.RpcError as e:
            print(f"\nDisconnected from Broker: {e.code()} - {e.details()}")
        except KeyboardInterrupt:
            print("\nStopped by the user (Ctrl+C).")

if __name__ == '__main__':
    run()
