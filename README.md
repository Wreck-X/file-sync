# Distributed File Synchronization System

This project implements a real-time file synchronization system that allows files to be kept in sync across multiple devices. It's similar in concept to services like Dropbox or Google Drive, but as a self-hosted solution.

## Features

- Real-time file synchronization across multiple devices
- Web-based user interface for file management
- WebSocket-based notifications for instant updates
- File change detection using file system watchers
- Conflict resolution using file timestamps and hash comparisons
- RESTful API for file operations
- Support for file uploads, downloads, and deletions

## Architecture

The system consists of two main components:

1. **Server**: A Node.js application that manages file storage, detects changes, and broadcasts updates to connected clients.
2. **Client**: A web-based interface that allows users to view, upload, download, and delete files.

### How It Works

- Files are stored in a designated directory on the server
- The server watches for file system changes using the `chokidar` library
- When a file is added, modified, or deleted, the change is detected and broadcast to all connected clients
- Clients receive real-time updates via WebSocket connections
- Each client has a unique ID to prevent echo effects (ignoring its own changes)
- Files are identified by their path, and changes are tracked using file hashes and timestamps

## Getting Started

### Prerequisites

- Node.js (v14 or later)
- npm (v6 or later)

### Installation

1. Clone the repository or download the source code
2. Install dependencies:

```bash
npm install express http ws path fs-extra chokidar crypto
```

### Configuration

The server can be configured by modifying the configuration object in `server.js`. The main configurable options include:

- `port`: The port number the server will listen on (default: 3000)
- `syncDirectory`: The directory where files will be stored and synchronized

### Running the Server

Start the server by running:

```bash
node server.js
```

The server will begin listening on the configured port (default: 3000) and will create the sync directory if it doesn't exist.

### Accessing the Client

Once the server is running, you can access the web client by opening a browser and navigating to:

```
http://localhost:3000
```

## API Reference

The system provides a RESTful API for file operations:

### Get File List

```
GET /api/files
```

Returns a JSON array of all files in the sync directory, including metadata.

### Upload File

```
POST /api/files/upload?path={filePath}
```

Uploads a file to the specified path. The request body should contain the file content.

### Delete File

```
DELETE /api/files?path={filePath}
```

Deletes the file at the specified path.

## WebSocket Protocol

The system uses WebSockets for real-time updates. The main message types are:

### Server to Client:

- `init`: Initial data with file list
- `file_change`: Notification of file changes (create, update, delete)
- `file_data`: File content transfer

### Client to Server:

- `sync_request`: Request for file synchronization
- `file_change`: Notification of client-side file changes

## Security Considerations

This implementation is designed as a demonstration and is not production-ready in terms of security. For a production environment, consider:

- Adding user authentication and authorization
- Implementing TLS/SSL encryption
- Adding file permission controls
- Implementing rate limiting
- Adding proper error handling and validation

## Extending the System

This system can be extended in several ways:

### Client Applications

- Desktop clients using Electron or similar frameworks
- Mobile applications using React Native or other mobile frameworks
- Command-line interface for scripted operations

### Additional Features

- User accounts and authentication
- Shared folders and collaboration features
- Version history and file recovery
- End-to-end encryption
- Bandwidth throttling
- Selective synchronization
- Conflict resolution UI
- Offline support with synchronization queues

## Limitations

- This implementation uses in-memory storage for file metadata, which would not persist across server restarts in a production environment
- Large files might cause memory issues as the entire file is loaded into memory during upload/download
- The conflict resolution strategy is simple and might not handle complex scenarios
- No support for folder operations (create, move, etc.)

## Troubleshooting

### Connection Issues

If the client cannot connect to the server:
- Check that the server is running
- Verify network connectivity
- Ensure no firewalls are blocking the connection

### File Synchronization Issues

If files are not synchronizing properly:
- Check the activity logs in the client interface
- Verify file system permissions
- Restart the server and refresh the client

## License

This project is licensed under the MIT License - see the LICENSE file for details.