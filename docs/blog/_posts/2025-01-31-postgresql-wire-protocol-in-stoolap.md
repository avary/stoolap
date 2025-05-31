---
layout: post
title: "How Stoolap Implements PostgreSQL Wire Protocol"
date: 2025-01-31
author: Semih Alev
excerpt: "Deep dive into Stoolap's PostgreSQL wire protocol implementation. Learn how we built compatibility with PostgreSQL clients while maintaining our embedded database architecture."
---

# How Stoolap Implements PostgreSQL Wire Protocol

The PostgreSQL wire protocol is the communication standard that allows clients to interact with PostgreSQL servers. By implementing this protocol, Stoolap enables any PostgreSQL-compatible client, driver, or ORM to work seamlessly with our database. Here's how we built it.

## Understanding the PostgreSQL Wire Protocol

The PostgreSQL protocol operates over TCP/IP and uses a message-based communication pattern. Key characteristics:

- **Message-based**: Each interaction is a typed message with a specific format
- **Binary protocol**: Efficient data representation
- **Stateful**: Maintains connection and transaction state
- **Extensible**: Supports prepared statements, cursors, and more

## Stoolap's Implementation

### Starting the Server

```go
// cmd/stoolap-pgserver/server.go
func (s *Server) Start() error {
    listener, err := net.Listen("tcp", s.bindAddr)
    if err != nil {
        return err
    }
    
    for {
        conn, err := listener.Accept()
        if err != nil {
            continue
        }
        go s.handleConnection(conn)
    }
}
```

### Message Flow

The typical connection flow:

1. **Startup**: Client sends startup message with parameters
2. **Authentication**: Server responds (we currently accept all connections)
3. **Ready**: Server signals readiness
4. **Query/Response**: Client sends queries, server returns results

### Parsing Client Messages

```go
// Read message type and length
msgType, _ := reader.ReadByte()
length := binary.BigEndian.Uint32(lengthBytes)

switch msgType {
case 'Q': // Simple query
    query := string(data[:len(data)-1]) // Remove null terminator
    s.handleQuery(conn, query)
    
case 'P': // Parse (prepared statement)
    s.handleParse(conn, data)
    
case 'B': // Bind
    s.handleBind(conn, data)
    
case 'E': // Execute
    s.handleExecute(conn, data)
}
```

### Query Execution

When a query arrives, we:

1. Parse the SQL using Stoolap's parser
2. Determine the query type
3. Execute through the appropriate engine path
4. Format results in PostgreSQL wire format

```go
func (s *Server) handleQuery(conn net.Conn, query string) {
    // Special handling for PostgreSQL system queries
    if strings.Contains(query, "pg_") {
        s.handleSystemQuery(conn, query)
        return
    }
    
    // Execute through Stoolap engine
    result, err := s.executor.Execute(query)
    if err != nil {
        s.sendError(conn, err)
        return
    }
    
    // Send results in PostgreSQL format
    s.sendQueryResult(conn, result)
}
```

### Transaction Management

PostgreSQL clients expect specific transaction semantics:

```go
// Handle BEGIN/COMMIT/ROLLBACK
case "BEGIN":
    conn.activeTx = s.db.Begin()
    s.sendCommandComplete(conn, "BEGIN")
    
case "COMMIT":
    if conn.activeTx != nil {
        conn.activeTx.Commit()
        conn.activeTx = nil
    }
    s.sendCommandComplete(conn, "COMMIT")
```

### Row Data Format

Converting Stoolap results to PostgreSQL format:

```go
func (s *Server) sendDataRow(conn net.Conn, row []interface{}) {
    buf := new(bytes.Buffer)
    buf.WriteByte('D') // DataRow message
    
    // Number of columns
    binary.Write(buf, binary.BigEndian, int16(len(row)))
    
    // Each column value
    for _, value := range row {
        if value == nil {
            binary.Write(buf, binary.BigEndian, int32(-1)) // NULL
        } else {
            data := formatValue(value)
            binary.Write(buf, binary.BigEndian, int32(len(data)))
            buf.Write(data)
        }
    }
    
    conn.Write(buf.Bytes())
}
```

## Handling PostgreSQL-Specific Features

### System Catalogs

PostgreSQL clients often query system catalogs:

```go
// Minimal pg_database implementation
if query == "SELECT oid, datname FROM pg_database" {
    s.sendRowDescription(conn, []Column{
        {Name: "oid", Type: OID},
        {Name: "datname", Type: TEXT},
    })
    s.sendDataRow(conn, []interface{}{1, "stoolap"})
    s.sendCommandComplete(conn, "SELECT 1")
}
```

### Type System Mapping

Mapping Stoolap types to PostgreSQL OIDs:

```go
var typeOIDMap = map[string]uint32{
    "INTEGER":   23,  // INT4
    "BIGINT":    20,  // INT8
    "TEXT":      25,  // TEXT
    "BOOLEAN":   16,  // BOOL
    "FLOAT":     701, // FLOAT8
    "TIMESTAMP": 1114, // TIMESTAMP
    "JSON":      114, // JSON
}
```

### Isolation Level Translation

```go
// PostgreSQL isolation levels to Stoolap
func translateIsolation(pgLevel string) string {
    switch pgLevel {
    case "READ COMMITTED":
        return "READ COMMITTED"
    case "REPEATABLE READ", "SERIALIZABLE":
        return "SNAPSHOT" // Map to our SNAPSHOT isolation
    default:
        return "READ COMMITTED"
    }
}
```

## Connection State Management

Each connection maintains state:

```go
type ClientConnection struct {
    conn         net.Conn
    db          *stoolap.DB
    currentTx   *stoolap.Tx
    preparedStmts map[string]*PreparedStatement
    portalMap    map[string]*Portal
    txIsolation  string
}
```

## Performance Optimizations

### Buffer Pooling
```go
var bufferPool = sync.Pool{
    New: func() interface{} {
        return new(bytes.Buffer)
    },
}

func getBuffer() *bytes.Buffer {
    return bufferPool.Get().(*bytes.Buffer)
}

func putBuffer(buf *bytes.Buffer) {
    buf.Reset()
    bufferPool.Put(buf)
}
```

### Efficient Message Writing
```go
// Pre-allocate message headers
var readyForQuery = []byte{'Z', 0, 0, 0, 5, 'I'}
var emptyQueryResponse = []byte{'I', 0, 0, 0, 4}

// Direct write for common responses
conn.Write(readyForQuery)
```

## Testing the Implementation

Connect with psql:
```bash
psql -h localhost -p 5432 -d stoolap

stoolap=# CREATE TABLE test (id INT, data TEXT);
CREATE TABLE

stoolap=# INSERT INTO test VALUES (1, 'Hello PostgreSQL!');
INSERT 0 1

stoolap=# SELECT * FROM test;
 id |       data        
----+------------------
  1 | Hello PostgreSQL!
(1 row)
```

Connect with Python:
```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="stoolap"
)

cur = conn.cursor()
cur.execute("SELECT version()")
print(cur.fetchone())
```

## Current Limitations and Future Work

### What Works Now
- Simple queries (SELECT, INSERT, UPDATE, DELETE)
- Transactions (BEGIN, COMMIT, ROLLBACK)
- Basic prepared statements
- Type conversions
- Multiple concurrent connections

### Future Enhancements
- SSL/TLS support
- Authentication mechanisms
- COPY protocol for bulk data
- Extended query protocol
- Streaming replication protocol

## Architecture Benefits

Implementing PostgreSQL wire protocol provides:

1. **Ecosystem Compatibility**: Use existing tools and libraries
2. **Language Support**: Any language with PostgreSQL driver works
3. **ORM Support**: Frameworks like GORM, SQLAlchemy work out of the box
4. **Tool Integration**: pgAdmin, DBeaver, and other tools just work

## Conclusion

By implementing the PostgreSQL wire protocol, Stoolap bridges the gap between embedded database simplicity and enterprise database compatibility. You get the lightweight nature of an embedded database with the universal connectivity of PostgreSQL.

This implementation showcases Stoolap's philosophy: sophisticated capabilities don't require complex deployments. Whether you connect through our native Go driver or any PostgreSQL client, you get the same powerful HTAP engine underneath.

---

*Try the PostgreSQL server yourself: `go install github.com/stoolap/stoolap/cmd/stoolap-pgserver@latest`*