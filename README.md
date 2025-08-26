# mini-leveldb

An educational implementation of LevelDB in Go. Learn how LSM trees, compaction, and storage engines work by exploring a clean, well-structured codebase.

## What You'll Learn

- **LSM Tree Architecture**: Multi-level compaction from L0 to L6
- **Write-Ahead Logging**: Binary WAL with CRC32 integrity
- **Memory-Mapped I/O**: Zero-copy file access patterns
- **Bloom Filters**: Probabilistic data structures for fast lookups
- **Storage Engine Design**: From memtables to persistent storage
- **Performance Optimization**: Batch operations and efficient I/O

## Quick Start

```bash
# Build and try it out
make build

# Basic operations
./build/minildb put key1 value1
./build/minildb get key1
./build/minildb flush
```

## Architecture

```mermaid
graph TD
    A["Put(key, value)"] --> B["Write-Ahead Log<br/>(WAL file)"]
    A --> C["MemTable<br/>(In-memory key-value map)"]
    
    D["Get(key)"] --> C
    C --> E{"Key found?"}
    E -->|Yes| F["Return value"]
    E -->|No| G["Search L0 SSTables"]
    
    C --> H["Flush (memtable to disk)"]
    H --> I["Level 0 SSTables"]
    
    I --> J["SSTable 0-A"]
    I --> K["SSTable 0-B"]
    
    G --> J
    G --> K
    G --> L{"Key found?"}
    L -->|Yes| F
    L -->|No| M["Search L1, L2, etc."]
    
    J --> N["Compaction merge"]
    K --> N
    N --> O["Level 1 SSTables"]
    
    O --> P["SSTable 1"]
    P --> Q["Compaction (next level)"]
    Q --> R["Level 2 SSTables"]
    R --> S["SSTable 2"]
    
    style A fill:#e1f5fe
    style D fill:#f3e5f5
    style C fill:#fff3e0
    style I fill:#e8f5e8
    style O fill:#e8f5e8
    style R fill:#e8f5e8
```

## Code Structure

- `db/` - Core database implementation
  - `db.go` - Main DB interface with multi-level compaction
  - `sstable.go` - SSTable format with mmap and bloom filters  
  - `wal.go` - Write-ahead log with binary format
  - `bloom.go` - Bloom filter implementation
- `cmd/` - CLI interface

## Testing

```bash
make test
```

## Why mini-leveldb?

This project focuses on **clarity over performance**. Each component is implemented to be:
- **Readable**: Clean, well-structured Go code
- **Educational**: Learn database internals step by step  
- **Complete**: All major LevelDB features included
- **Testable**: Comprehensive test coverage

Perfect for understanding how modern storage engines like RocksDB, TiKV, and CockroachDB work under the hood.

## License

MIT License
