// Distributed File Synchronization System
// This implementation demonstrates a Node.js based file synchronization system
// that allows real-time file synchronization across multiple devices

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs-extra');
const chokidar = require('chokidar');
const crypto = require('crypto');
const ip = require('ip');

// Configuration
const config = {
    port: process.env.PORT || 3000,
    syncDirectory: path.join(__dirname, 'sync_dir'),
    clientId: crypto.randomUUID(), // Unique identifier for this server instance
    maxClients: 50, // Maximum number of clients allowed
    startPort: 3001, // Starting port for client assignment
    endPort: 3050, // Ending port for client assignment
};

// Ensure sync directory exists
fs.ensureDirSync(config.syncDirectory);

// Create Express app and HTTP server
const app = express();
const server = http.createServer(app);

// File metadata database (in-memory for demo)
// In a production system, this would be a persistent database
const fileDatabase = new Map();

// Client management system
const activeClients = new Map(); // Maps clientId to client info
const portAssignments = new Map(); // Maps port numbers to client IDs

// WebSocket server for real-time updates
const wss = new WebSocket.Server({ server });

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));
app.use('/sync', express.static(config.syncDirectory));

// API endpoints
app.use(express.json());

// Get list of all files
app.get('/api/files', (req, res) => {
    const files = Array.from(fileDatabase.values());
    res.json(files);
});

// Get list of active clients
app.get('/api/clients', (req, res) => {
    const clients = Array.from(activeClients.values()).map(client => ({
        id: client.id,
        ipAddress: client.ipAddress,
        port: client.port,
        connectedAt: client.connectedAt,
        lastActive: client.lastActive,
    }));
    res.json(clients);
});

// Register as a new client and get port assignment
app.post('/api/register', (req, res) => {
    const requestedName = req.body.name || 'Anonymous';
    const remoteAddress = req.ip || req.connection.remoteAddress;
    const parsedIp = remoteAddress.replace(/^::ffff:/, ''); // Handle IPv4 mapped to IPv6
    
    // Generate a new client ID
    const newClientId = crypto.randomUUID();
    
    // Find an available port
    let assignedPort = null;
    for (let port = config.startPort; port <= config.endPort; port++) {
        if (!portAssignments.has(port)) {
            assignedPort = port;
            break;
        }
    }
    
    if (assignedPort === null) {
        return res.status(503).json({
            error: 'No available ports for new clients'
        });
    }
    
    // Register the new client
    const clientInfo = {
        id: newClientId,
        name: requestedName,
        ipAddress: parsedIp,
        port: assignedPort,
        connectedAt: new Date(),
        lastActive: new Date(),
    };
    
    activeClients.set(newClientId, clientInfo);
    portAssignments.set(assignedPort, newClientId);
    
    console.log(`Registered new client: ${requestedName} (${newClientId}) at port ${assignedPort}`);
    
    // Return the client info including their assigned port
    res.status(201).json({
        clientId: newClientId,
        assignedPort: assignedPort,
        serverAddress: ip.address(),
        serverPort: config.port
    });
});

// File upload endpoint
app.post('/api/files/upload', express.raw({ limit: '100mb', type: '*/*' }), async (req, res) => {
    const filePath = req.query.path;
    const clientId = req.query.clientId || 'unknown';
    
    if (!filePath) {
        return res.status(400).json({ error: 'File path is required' });
    }

    // Update client's last active timestamp if it exists
    if (activeClients.has(clientId)) {
        const client = activeClients.get(clientId);
        client.lastActive = new Date();
        activeClients.set(clientId, client);
    }

    const fullPath = path.join(config.syncDirectory, filePath);
    try {
        await fs.ensureDir(path.dirname(fullPath));
        await fs.writeFile(fullPath, req.body);
        
        const stats = await fs.stat(fullPath);
        const fileHash = crypto.createHash('md5').update(req.body).digest('hex');
        
        const fileInfo = {
            path: filePath,
            size: stats.size,
            lastModified: stats.mtime,
            hash: fileHash,
            uploadedBy: clientId
        };
        
        fileDatabase.set(filePath, fileInfo);
        
        // Broadcast the file change to other clients
        broadcastFileChange({
            type: 'update',
            source: clientId,
            file: fileInfo
        });
        
        res.status(201).json(fileInfo);
    } catch (err) {
        console.error('Error uploading file:', err);
        res.status(500).json({ error: 'Failed to upload file' });
    }
});

// Delete file endpoint
app.delete('/api/files', async (req, res) => {
    const filePath = req.query.path;
    const clientId = req.query.clientId || 'unknown';
    
    if (!filePath) {
        return res.status(400).json({ error: 'File path is required' });
    }
    
    // Update client's last active timestamp if it exists
    if (activeClients.has(clientId)) {
        const client = activeClients.get(clientId);
        client.lastActive = new Date();
        activeClients.set(clientId, client);
    }
    
    const fullPath = path.join(config.syncDirectory, filePath);
    try {
        await fs.remove(fullPath);
        fileDatabase.delete(filePath);
        
        // Broadcast the file deletion to other clients
        broadcastFileChange({
            type: 'delete',
            source: clientId,
            path: filePath
        });
        
        res.status(204).end();
    } catch (err) {
        console.error('Error deleting file:', err);
        res.status(500).json({ error: 'Failed to delete file' });
    }
});

// Heartbeat endpoint for clients to maintain their active status
app.post('/api/heartbeat', (req, res) => {
    const clientId = req.body.clientId;
    
    if (!clientId || !activeClients.has(clientId)) {
        return res.status(404).json({ error: 'Client not found' });
    }
    
    const client = activeClients.get(clientId);
    client.lastActive = new Date();
    activeClients.set(clientId, client);
    
    res.status(200).json({
        timestamp: new Date(),
        activeClients: activeClients.size
    });
});

// Client disconnect endpoint
app.post('/api/disconnect', (req, res) => {
    const clientId = req.body.clientId;
    
    if (!clientId || !activeClients.has(clientId)) {
        return res.status(404).json({ error: 'Client not found' });
    }
    
    const client = activeClients.get(clientId);
    
    // Free up the port assignment
    portAssignments.delete(client.port);
    activeClients.delete(clientId);
    
    console.log(`Client disconnected: ${client.name} (${clientId}) from port ${client.port}`);
    
    // Broadcast client disconnection to all clients
    broadcastClientEvent({
        type: 'client_disconnected',
        clientId: clientId,
        clientName: client.name
    });
    
    res.status(200).json({ success: true });
});

// WebSocket connection handling
wss.on('connection', (ws, req) => {
    let clientId = null;
    
    // Extract client ID from URL if present
    const url = new URL(req.url, `http://${req.headers.host}`);
    clientId = url.searchParams.get('clientId');
    
    console.log(`WebSocket connection from ${req.socket.remoteAddress}, clientId: ${clientId || 'unknown'}`);
    
    // Store client ID and websocket in the socket for easy reference
    ws.clientId = clientId;
    
    // Update client's last active timestamp if it exists
    if (clientId && activeClients.has(clientId)) {
        const client = activeClients.get(clientId);
        client.lastActive = new Date();
        client.ws = ws; // Store the WebSocket connection
        activeClients.set(clientId, client);
        
        // Broadcast client connection to all clients
        broadcastClientEvent({
            type: 'client_connected',
            clientId: clientId,
            clientName: client.name
        });
    }
    
    // Send initial file list to new client
    ws.send(JSON.stringify({
        type: 'init',
        clientId: clientId,
        files: Array.from(fileDatabase.values()),
        clients: Array.from(activeClients.values()).map(client => ({
            id: client.id,
            name: client.name,
            ipAddress: client.ipAddress,
            port: client.port
        }))
    }));
    
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            // Update client's last active timestamp
            if (clientId && activeClients.has(clientId)) {
                const client = activeClients.get(clientId);
                client.lastActive = new Date();
                activeClients.set(clientId, client);
            }
            
            handleClientMessage(data, ws, clientId);
        } catch (err) {
            console.error('Error processing message:', err);
        }
    });
    
    ws.on('close', () => {
        console.log(`WebSocket closed for client: ${clientId || 'unknown'}`);
        
        // Do not remove from active clients here
        // Client will explicitly disconnect via API
    });
});

// Handle incoming client messages
function handleClientMessage(message, ws, clientId) {
    switch (message.type) {
        case 'sync_request':
            // Client requests specific file
            sendFileToClient(message.path, ws);
            break;
        
        case 'file_change':
            // Only process if from a different source
            if (message.source !== config.clientId) {
                processRemoteFileChange(message);
            }
            break;
            
        case 'heartbeat':
            // Client heartbeat
            // Already handled in the message listener
            break;
            
        default:
            console.log('Unknown message type:', message.type);
    }
}

// Send file data to client
async function sendFileToClient(filePath, ws) {
    const fullPath = path.join(config.syncDirectory, filePath);
    try {
        const data = await fs.readFile(fullPath);
        ws.send(JSON.stringify({
            type: 'file_data',
            path: filePath,
            data: data.toString('base64')
        }));
    } catch (err) {
        console.error('Error sending file to client:', err);
    }
}

// Process remote file changes
async function processRemoteFileChange(change) {
    if (change.type === 'update') {
        const fileInfo = change.file;
        fileDatabase.set(fileInfo.path, fileInfo);
        
        // Actual file data would be downloaded separately as needed
        console.log(`Remote file updated: ${fileInfo.path}`);
    } else if (change.type === 'delete') {
        fileDatabase.delete(change.path);
        
        // Remove local file
        const fullPath = path.join(config.syncDirectory, change.path);
        try {
            await fs.remove(fullPath);
            console.log(`Remote file deleted: ${change.path}`);
        } catch (err) {
            console.error('Error deleting local file:', err);
        }
    }
}

// Broadcast file changes to all connected clients
function broadcastFileChange(change) {
    const message = JSON.stringify({
        type: 'file_change',
        ...change
    });
    
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

// Broadcast client events (connect/disconnect) to all connected clients
function broadcastClientEvent(event) {
    const message = JSON.stringify({
        type: 'client_event',
        ...event
    });
    
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

// Periodically check for inactive clients
setInterval(() => {
    const now = new Date();
    const inactiveThreshold = 5 * 60 * 1000; // 5 minutes
    
    activeClients.forEach((client, clientId) => {
        const lastActive = client.lastActive;
        if ((now - lastActive) > inactiveThreshold) {
            console.log(`Client inactive, removing: ${client.name} (${clientId})`);
            
            // Free up the port assignment
            portAssignments.delete(client.port);
            activeClients.delete(clientId);
            
            // Broadcast client disconnection
            broadcastClientEvent({
                type: 'client_disconnected',
                clientId: clientId,
                clientName: client.name,
                reason: 'timeout'
            });
        }
    });
}, 60 * 1000); // Check every minute

// File system watcher to detect local changes
const watcher = chokidar.watch(config.syncDirectory, {
    ignored: /(^|[\/\\])\../, // ignore dotfiles
    persistent: true,
    ignoreInitial: true,
    awaitWriteFinish: true
});

// Handle file system events
watcher.on('add', handleFileAdded);
watcher.on('change', handleFileChanged);
watcher.on('unlink', handleFileDeleted);

// Handle new file detected
async function handleFileAdded(filePath) {
    await handleFileChanged(filePath);
}

// Handle file change detected
async function handleFileChanged(filePath) {
    try {
        const relativePath = path.relative(config.syncDirectory, filePath);
        const stats = await fs.stat(filePath);
        const data = await fs.readFile(filePath);
        const fileHash = crypto.createHash('md5').update(data).digest('hex');
        
        const fileInfo = {
            path: relativePath,
            size: stats.size,
            lastModified: stats.mtime,
            hash: fileHash,
            uploadedBy: config.clientId
        };
        
        // Check if this is a new version
        const existingFile = fileDatabase.get(relativePath);
        if (!existingFile || existingFile.hash !== fileInfo.hash) {
            fileDatabase.set(relativePath, fileInfo);
            
            // Broadcast file change to all clients
            broadcastFileChange({
                type: 'update',
                source: config.clientId,
                file: fileInfo
            });
            
            console.log(`Local file changed: ${relativePath}`);
        }
    } catch (err) {
        console.error('Error processing file change:', err);
    }
}

// Handle file deletion
function handleFileDeleted(filePath) {
    try {
        const relativePath = path.relative(config.syncDirectory, filePath);
        fileDatabase.delete(relativePath);
        
        // Broadcast file deletion to all clients
        broadcastFileChange({
            type: 'delete',
            source: config.clientId,
            path: relativePath
        });
        
        console.log(`Local file deleted: ${relativePath}`);
    } catch (err) {
        console.error('Error processing file deletion:', err);
    }
}

// Scan existing files on startup
async function scanExistingFiles() {
    try {
        const files = await fs.readdir(config.syncDirectory, { withFileTypes: true, recursive: true });
        
        for (const file of files) {
            if (file.isFile()) {
                const fullPath = path.join(file.path, file.name);
                const relativePath = path.relative(config.syncDirectory, fullPath);
                
                const stats = await fs.stat(fullPath);
                const data = await fs.readFile(fullPath);
                const fileHash = crypto.createHash('md5').update(data).digest('hex');
                
                fileDatabase.set(relativePath, {
                    path: relativePath,
                    size: stats.size,
                    lastModified: stats.mtime,
                    hash: fileHash,
                    uploadedBy: config.clientId
                });
            }
        }
        
        console.log(`Scanned ${fileDatabase.size} existing files`);
    } catch (err) {
        console.error('Error scanning existing files:', err);
    }
}

// Initialize and start the server
async function init() {
    await scanExistingFiles();
    
    // Display server information
    const serverIp = ip.address();
    
    server.listen(config.port, '0.0.0.0', () => {
        console.log(`File sync server running on port ${config.port}`);
        console.log(`Server accessible at http://${serverIp}:${config.port}`);
        console.log(`Sync directory: ${config.syncDirectory}`);
        console.log(`Server ID: ${config.clientId}`);
        console.log(`Client port range: ${config.startPort} - ${config.endPort}`);
    });
}

// Add graceful shutdown handling
process.on('SIGINT', () => {
    console.log('Shutting down server...');
    server.close(() => {
        console.log('Server shut down successfully');
        process.exit(0);
    });
});

init().catch(err => console.error('Initialization error:', err));