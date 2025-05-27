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

const config = {
    port: process.env.PORT || 3000,
    syncDirectory: path.join(__dirname, 'sync_dir'),
    clientId: crypto.randomUUID(),
    maxClients: 50,
    startPort: 3001,
    endPort: 3050,
};

// Ensure sync directory exists
fs.ensureDirSync(config.syncDirectory);

// Create Express app and HTTP server
const app = express();
const server = http.createServer(app);

// File metadata database (in-memory for demo)
const fileDatabase = new Map();

// Client management system
const activeClients = new Map(); // Maps clientId to client info
const portAssignments = new Map(); // Maps port numbers to client IDs
const activeConnections = new Map(); // NEW: Maps clientId to WebSocket connection

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
        name: client.name,
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
    const remoteAddress = req.ip || req.connection.remoteAddress || req.socket.remoteAddress;
    const parsedIp = remoteAddress ? remoteAddress.replace(/^::ffff:/, '') : 'unknown';
    
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
    
    // Broadcast new client registration
    broadcastClientEvent({
        eventType: 'client_registered',
        client: {
            id: clientInfo.id,
            name: clientInfo.name,
            ipAddress: clientInfo.ipAddress,
            port: clientInfo.port
        }
    });
    
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

    // Update client's last active timestamp
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
        
        const uploaderName = activeClients.has(clientId) ? activeClients.get(clientId).name : 'Unknown';
        
        const fileInfo = {
            path: filePath,
            size: stats.size,
            lastModified: stats.mtime,
            hash: fileHash,
            uploadedBy: clientId,
            uploaderName: uploaderName
        };
        
        fileDatabase.set(filePath, fileInfo);
        
        // Broadcast the file change
        broadcastFileChange({
            eventType: 'update',
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
    
    // Update client's last active timestamp
    if (activeClients.has(clientId)) {
        const client = activeClients.get(clientId);
        client.lastActive = new Date();
        activeClients.set(clientId, client);
    }
    
    const fullPath = path.join(config.syncDirectory, filePath);
    try {
        await fs.remove(fullPath);
        fileDatabase.delete(filePath);
        
        // Broadcast the file deletion
        broadcastFileChange({
            eventType: 'delete',
            source: clientId,
            path: filePath
        });
        
        res.status(204).end();
    } catch (err) {
        console.error('Error deleting file:', err);
        res.status(500).json({ error: 'Failed to delete file' });
    }
});

// Heartbeat endpoint
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
    
    // Free up resources
    portAssignments.delete(client.port);
    activeClients.delete(clientId);
    activeConnections.delete(clientId);
    
    console.log(`Client disconnected: ${client.name} (${clientId}) from port ${client.port}`);
    
    // Broadcast disconnection
    broadcastClientEvent({
        eventType: 'client_disconnected',
        clientId: clientId,
        clientName: client.name
    });
    
    res.status(200).json({ success: true });
});

// WebSocket connection handling
wss.on('connection', (ws, req) => {
    // Extract client ID from URL
    const urlParams = new URLSearchParams(req.url.replace(/^\/|\?.*$/, ''));
    const clientId = urlParams.get('clientId');
    
    if (!clientId || !activeClients.has(clientId)) {
        console.log(`Invalid WebSocket connection attempt: clientId ${clientId || 'missing'}`);
        ws.close(1008, 'Invalid client ID');
        return;
    }
    
    console.log(`WebSocket connection from ${req.socket.remoteAddress}, clientId: ${clientId}, active connections: ${wss.clients.size}`);
    
    // Store connection
    ws.clientId = clientId;
    activeConnections.set(clientId, ws);
    
    // Update client info
    if (activeClients.has(clientId)) {
        const client = activeClients.get(clientId);
        client.lastActive = new Date();
        activeClients.set(clientId, client);
        
        // Broadcast client connection
        broadcastClientEvent({
            eventType: 'client_connected',
            client: {
                id: client.id,
                name: client.name,
                ipAddress: client.ipAddress,
                port: client.port
            }
        }, clientId);
    }
    
    // Send initial data
    ws.send(JSON.stringify({
        type: 'init',
        clientId,
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
            const data = JSON.parse(message.toString());
            if (data.type === 'heartbeat') {
                ws.send(JSON.stringify({ type: 'heartbeat_ack', timestamp: Date.now() }));
                if (clientId && activeClients.has(clientId)) {
                    const client = activeClients.get(clientId);
                    client.lastActive = Date.now();
                    activeClients.set(clientId, client);
                }
            }
            handleClientMessage(data, ws);
        } catch (error) {
            console.error(`Error processing message for client ${clientId}:`, error);
        }
    });
    
    ws.on('close', () => {
        console.log(`WebSocket closed for clientId: ${clientId}`);
        activeConnections.delete(clientId);
    });
    
    ws.on('error', (error) => {
        console.error(`WebSocket error for client ${clientId}:`, error);
        activeConnections.delete(clientId);
    });
});

// Handle incoming client messages
function handleClientMessage(message, ws) {
    switch (message.type) {
        case 'sync_request':
            sendFileToClient(message.path, ws);
            break;
        case 'change':
        case 'file_change':
            if (message.type === 'file_change') {
                console.warn(`Received legacy 'file_change' message from client, should use 'change':`, message);
            }
            if (message.source !== config.clientId) {
                processRemoteFileChange(message);
            }
            break;
        case 'heartbeat':
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
        console.error('Error sending file:', err);
    }
}

// Process remote file changes
async function processRemoteFileChange(change) {
    if (change.eventType === 'update') {
        const fileInfo = change.file;
        fileDatabase.set(fileInfo.path, fileInfo);
        console.log(`Remote file updated: ${fileInfo.path}`);
    } else if (change.eventType === 'delete') {
        fileDatabase.delete(change.path);
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
        type: 'change',
        eventType: change.eventType,
        source: change.source,
        file: change.file,
        path: change.path
    });
    let clientCount = 0;
    const sentTo = [];
    activeConnections.forEach((ws, clientId) => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(message);
            clientCount++;
            sentTo.push(clientId);
        }
    });
    console.log(`Broadcasting file change to ${clientCount} clients, sent to: ${sentTo.join(', ')}`, change);
}

// Broadcast client events to all connected clients
function broadcastClientEvent(event, excludeClientId = null) {
    const message = JSON.stringify({
        type: 'client',
        eventType: event.eventType,
        client: event.client,
        clientId: event.clientId,
        clientName: event.clientName
    });
    let clientCount = 0;
    const sentTo = [];
    activeConnections.forEach((ws, clientId) => {
        if (ws.readyState === WebSocket.OPEN && clientId !== excludeClientId) {
            ws.send(message);
            clientCount++;
            sentTo.push(clientId);
        }
    });
    console.log(`Broadcasting client event to ${clientCount} clients: ${JSON.stringify(event.eventType)}`, `sent to: ${sentTo.join(', ')}`);
}

// Periodically check for inactive clients
setInterval(() => {
    const now = new Date();
    const inactiveThreshold = 300000;
    
    activeClients.forEach((client, clientId) => {
        if ((now - client.lastActive) > inactiveThreshold) {
            console.log(`Client inactive, removing: ${client.name} (${clientId})`);
            portAssignments.delete(client.port);
            activeClients.delete(clientId);
            activeConnections.delete(clientId);
            broadcastClientEvent({
                eventType: 'client_disconnected',
                clientId: clientId,
                clientName: client.name,
                reason: 'timeout'
            });
        }
    });
}, 60000);

// File system watcher
const watcher = chokidar.watch(config.syncDirectory, {
    ignored: /(^|[\/\\])\../,
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
            uploadedBy: config.clientId,
            uploaderName: 'Server'
        };
        
        if (!fileDatabase.get(relativePath) || fileDatabase.get(relativePath).hash !== fileInfo.hash) {
            fileDatabase.set(relativePath, fileInfo);
            broadcastFileChange({
                eventType: 'update',
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
        broadcastFileChange({
            eventType: 'delete',
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
                    uploadedBy: config.clientId,
                    uploaderName: 'Server'
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
    
    const serverIp = ip.address();
    
    server.listen(config.port, '0.0.0.0', () => {
        console.log(`File sync server running on port ${config.port}`);
        console.log(`Server accessible at http://${serverIp}:${config.port}`);
        console.log(`Sync directory: ${config.syncDirectory}`);
        console.log(`Server ID: ${config.clientId}`);
        console.log(`Client port range: ${config.startPort} - ${config.endPort}`);
    });
}

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('Shutting down server...');
    server.close(() => {
        console.log('Server shut down successfully');
        process.exit(0);
    });
});

init().catch(err => console.error('Initialization error:', err));