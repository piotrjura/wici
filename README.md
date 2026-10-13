# Wici

A small protocol to send messages between two paired devices. For example,
an AI agent on a Mac sends its output to a phone, and the phone sends back
commands and approvals.

```mermaid
flowchart LR
    subgraph A["Device A: for example an agent on a Mac"]
        AA[App] <--> AQ[("Local queue<br/>SQLite")]
    end
    subgraph S["Wici server: relay"]
        SR[Routing] <--> SD[("Message store<br/>SQLite or PostgreSQL")]
    end
    subgraph B["Device B: for example a phone"]
        BQ[("Local queue<br/>SQLite")] <--> BA[App]
    end

    AQ <-->|"WebSocket<br/>encrypted messages"| SR
    SR <-->|"WebSocket<br/>encrypted messages"| BQ
```

The path of one message:

```mermaid
sequenceDiagram
    participant A as Device A
    participant S as Server
    participant B as Device B

    A->>A: Encrypt and save in the queue
    A->>S: Send
    S->>S: Save in the database
    S-->>A: Ack
    S->>B: Push
    B->>B: Save in the queue
    B-->>S: Ack
    S->>S: Delete the content
    Note over A,B: No ack: resend with the same ID. The server drops duplicates.<br/>B offline: the server keeps the message and pushes it on reconnect.<br/>Replies (status, approvals, output) take the same path back.
```

- Devices pair once with a QR code or a link.
- Messages are end-to-end encrypted. The server cannot read them.
- Each device keeps a local queue. A lost connection loses no messages.
- A retry never runs a command twice.

Status: early. The server, the Rust client, and the Swift package work.
Read the [plan](docs/project-plan.md) for the details and the next steps.

## Run the server

```sh
WICI_DATABASE_URL=sqlite:wici.db cargo run --release -p wici-server
```

More: [development](docs/development.md), [code map](docs/code-map.md).

## License

Not chosen yet.
