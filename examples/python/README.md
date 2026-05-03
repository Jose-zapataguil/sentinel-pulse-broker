# Python Examples for Sentinel Pulse Broker

This directory contains Python client examples for interacting with the Sentinel Pulse Broker using gRPC.

## Prerequisites

- Python 3.8 or higher
- Access to the broker protobuf definition (see `src/main/protobuf/broker/broker.proto` in the project root)

## Environment Setup

Follow these steps to set up the Python environment:

### 1. Create the virtual environment

```bash
python -m venv .venv
```

### 2. Activate the virtual environment

**Windows:**
```bash
venv\Scripts\activate
```

**Linux/MacOS:**
```bash
source venv/bin/activate
```

### 3. Install required libraries

```bash
pip install grpcio grpcio-tools
```

### 4. Copy the protobuf definition

Copy the `broker.proto` file from the project to your environment directory:

```
src/main/protobuf/broker/broker.proto  ->  <your_directory>/broker.proto
```

### 5. Generate gRPC Python files

From your virtual environment directory, run:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. broker.proto
```

This generates:
- `broker_pb2.py` - Protocol Buffers message definitions
- `broker_pb2_grpc.py` - gRPC service stubs

## Running the Examples

Make sure the broker is running before executing the client scripts.

### Producer (Publisher)

The producer script allows you to publish messages to a channel interactively:

```bash
python producer.py
```

**Usage:**
1. Enter the channel name (default: `example-channel`)
2. Type your message and press Enter
3. Type `exit` to close the connection

The first message must contain metadata (channel name and TTL), subsequent messages contain the actual data.

### Consumer (Subscriber)

The consumer script subscribes to a channel and receives published messages:

```bash
python consumer.py
```

**Usage:**
1. The script automatically subscribes to `example-channel`
2. Set `all_messages = True` to receive previously stored messages
3. Set `all_messages = False` to only receive new messages

Press Ctrl+C to stop the consumer.

## API Reference

### PublishRequest

The `PublishRequest` message uses a oneof field:
- First message: must include `metadata` with channel and TTL
- Subsequent messages: include `data` with the message bytes

```python
PublishRequest(metadata=PublishMetadata(channel="my-channel", ttl=5000))
PublishRequest(data=b"Hello, world!")
```

### PullRequest

```python
PullRequest(channel="my-channel", all_messages=True)
```

| Field          | Type   | Description                        |
|----------------|--------|------------------------------------|
| `channel`      | string | Channel name to subscribe to       |
| `all_messages` | bool   | Whether to receive stored messages |

### PullResponse

```python
response = stream_messages.__next__()
channel = response.channel
payload = response.payload.decode('utf-8')
```

## Example: Running Both

Terminal 1 (Consumer):
```bash
python consumer.py
```

Terminal 2 (Producer):
```bash
python producer.py
```

Now type messages in the producer terminal and see them appear in the consumer terminal.

## Troubleshooting

- **ModuleNotFoundError**: Ensure you generated the protobuf files in the correct directory
- **Connection refused**: Ensure the broker is running on localhost:8080
- **Channel errors**: Check that the channel name matches in both producer and consumer