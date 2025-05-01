# P2P File Synchronization

A lightweight peer-to-peer file synchronization script that enables bidirectional synchronization between two directories, either on the same machine or across different machines on a network.

## Features

- **Bidirectional synchronization** between two peers
- **Real-time file change detection** using watchdog
- **Periodic synchronization** every 5 seconds
- **MD5 hash comparison** to avoid unnecessary transfers
- **Conflict resolution** based on modification timestamps
- **Proper handling of empty files**
- **Automatic directory creation**
- Handles file **additions**, **modifications**, and **deletions**

## Prerequisites

- Python 3.6 or higher
- `watchdog` library for file change detection

## Installation

1. Install the required Python package:

```bash
pip install watchdog
```

2. Download the `p2p_sync.py` script

## Basic Usage

Run the script on two different locations (peers) with different port configurations:

### On Peer 1:

```bash
python p2p_sync.py --local-port 8001 --remote-port 8002 --dir ./shared_dir1
```

### On Peer 2:

```bash
python p2p_sync.py --local-port 8002 --remote-port 8001 --dir ./shared_dir2
```

## Command Line Options

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| `--local-port` | Local port for the server to listen on | - | Yes |
| `--remote-port` | Remote port to connect to | - | Yes |
| `--remote-host` | Remote host to connect to | localhost | No |
| `--dir` | Directory to synchronize | - | Yes |
| `--initial-sync-delay` | Delay in seconds before initial sync | 3 | No |

## How It Works

### Architecture

The script implements a peer-to-peer architecture where each peer:
1. Runs a server to respond to sync requests
2. Monitors the local directory for changes
3. Periodically checks for changes on the remote peer
4. Transfers files in both directions as needed

### Synchronization Logic

1. **File Indexing**:
   - Creates an index of all files in the local directory
   - For each file, records size, modification time, and MD5 hash

2. **Change Detection**:
   - Uses watchdog to detect real-time file system changes
   - Schedules synchronization when changes are detected

3. **Conflict Resolution**:
   - Compares file hashes to detect changes
   - Uses file modification times to resolve conflicts
   - The newer version of a file always wins

4. **File Deletion**:
   - Only deletes files if they were deleted on the remote peer
   - Preserves newly created local files
   - Special protection for empty files

### Network Protocol

The script uses a simple TCP-based protocol:
- JSON messages for commands and metadata
- Raw binary transfers for file data
- File size prefixing to handle boundaries

## Special Handling

### Empty Files

Empty files (0 bytes) receive special handling:
- Proper indexing and synchronization
- Protected from accidental deletion
- Fully tracked with detailed logging

### Hidden Files

- Files beginning with `.` are ignored by the sync process
- The `.last_sync` file tracks synchronization timestamps
- Temporary `.tmp` files are used during transfers and ignored

## Log Messages

The script provides detailed logging to help diagnose synchronization issues:

- **File change detection**: "File change detected: [path] - Event type: [type]"
- **Empty file tracking**: "Indexing empty file: [path]"
- **Sync actions**: "Downloaded [path] ([size] bytes)" or "Deleted [path]"
- **Error messages**: Various error conditions with details

## Advanced Usage

### Synchronizing Between Different Machines

To synchronize between different machines on the same network:

```bash
# On Machine A
python p2p_sync.py --local-port 8001 --remote-port 8002 --remote-host 192.168.1.2 --dir ./shared_dir1

# On Machine B (with IP 192.168.1.2)
python p2p_sync.py --local-port 8002 --remote-port 8001 --remote-host 192.168.1.1 --dir ./shared_dir2
```

### Firewall Configuration

Ensure that the ports used for synchronization are open on both machines:
- Allow incoming TCP connections on the `--local-port`
- Allow outgoing TCP connections to the `--remote-port`

## Limitations

- Designed for synchronization between exactly two peers
- No encryption of data in transit (use VPN or SSH tunneling for security)
- No bandwidth throttling
- No support for symbolic links or special files
- Not optimized for very large files or directories with thousands of files

## Troubleshooting

### Connection Issues

If peers cannot connect:
1. Verify that both scripts are running
2. Check that `--local-port` and `--remote-port` are set correctly
3. Ensure firewalls allow traffic on the specified ports
4. Verify network connectivity between peers

### Synchronization Issues

If files are not synchronizing properly:
1. Check log messages for errors
2. Verify that file paths don't contain special characters
3. Ensure both peers have read/write permissions for their directories
4. Try increasing the `--initial-sync-delay` parameter

## Development

### Future Improvements

- Support for more than two peers
- Bandwidth throttling
- Delta transfers for large files
- Encryption and authentication
- GUI interface
- Conflict resolution with manual intervention
- Support for symbolic links and special files

## License

This script is provided as-is for educational and personal use.