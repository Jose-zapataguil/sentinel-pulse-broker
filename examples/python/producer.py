import grpc
import time

import broker_pb2
import broker_pb2_grpc

keepalive_options = [
        ('grpc.keepalive_time_ms', 10000),             
        ('grpc.keepalive_timeout_ms', 5000),           
        ('grpc.keepalive_permit_without_calls', True), 
        ('grpc.http2.max_pings_without_data', 0)
    ]

def run():

    print("Connected to the Broker in localhost:8080...")
    with grpc.insecure_channel('localhost:8080', options = keepalive_options) as channel:

        stub = broker_pb2_grpc.ProducerServiceStub(channel)

        # Send messages captured by the user input
        def interactive_generator():
            try:
                print(" -> Sending connection metadata...")

                metadata_request = broker_pb2.PublishRequest(
                    metadata=broker_pb2.PublishMetadata(
                        channel="example-channel",
                        ttl=10000
                    )
                )
                yield metadata_request

                print("   Connected, You can now write messages.")
                print("   (Type 'exit' and press Enter to close the connection)\n")

                while True:
                    text = input("💬 Message: ")

                    if text.strip().lower() == 'exit':
                        print("Clossing stream...")
                        break

                    if text.strip() == '':
                        continue

                    yield broker_pb2.PublishRequest(
                        data=text.encode('utf-8')
                    )

            except KeyboardInterrupt:
                print("\n   Transmission interrupted by the user.")

            except Exception as e:
                print(f"   Internal error: {e}")
                raise

        print("Starting streaming transmission...")
        try:
            summary = stub.Push(interactive_generator())

            print("\n--- Response ---")
            print(f"Success: {summary.success}")
            print(f"Messages sent: {summary.count}")

        except grpc.RpcError as e:
            print(f"Error gRPC: {e.code()} - {e.details()}")

if __name__ == '__main__':
    run()
