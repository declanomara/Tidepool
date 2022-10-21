import socket
import json

telemetry_format = {
        'data' : {
            'gatherers': {
                'total': -1,
                'rate': -1.0
            },
            'processors': {
                'total': -1,
                'rate': -1.0
            },
            'recorders': {
                'total': -1,
                'rate': -1.0
            }
        },
        'server': {
            'proc_counts': {
                'gatherers': -1,
                'processors': -1,
                'recorders': -1
            },
            'queues': {
                'unprocessed': {
                    'size': -1,
                    'average': -1
                },
                'unsaved': {
                    'size': -1,
                    'average': -1
                }
            },
            'uptime': -1,
        }
    }


def expose_telemetry(exposed_telemetry: dict, telemetry: dict = None) -> None:
    host = socket.gethostname()
    port = 65001    # TODO: Auto-assign port to avoid collision with other software

    server_socket = socket.socket()
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))  # bind host address and port together

    # Configure how many client the server can listen simultaneously
    server_socket.listen(10)
    while True:
        conn, address = server_socket.accept()

        while True:
            # Data does not matter, server always responds with telemetry data
            data = conn.recv(1024).decode()
            if not data:
                break

            response = json.dumps(exposed_telemetry.copy())  # Must copy as shared dict is not JSON serializable
            conn.send(response.encode('utf-8'))

        if telemetry is not None:
            telemetry['action_count'] += 1

        conn.close()