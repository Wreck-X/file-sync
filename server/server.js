const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs-extra');
const chokidar = require('chokidar');
const crypto = require('crypto');
const ip = require('ip');
const mongoose = require('mongoose');
const { Dropbox } = require('dropbox');
const fetch = require('node-fetch');
require('dotenv').config();

// Configuration
const config = {
    port: process.env.PORT || 3000,
    syncDirectory: path.join(__dirname, 'sync_dir'),
    clientId: crypto.randomUUID(),
    maxClients: 50,
    startPort: 3001,
    endPort: 3050,
};

// Initialize Dropbox
const dbx = new Dropbox({
    clientId: process.env.DROPBOX_APP_KEY,
    clientSecret: process.env.DROPBOX_APP_SECRET,
    refreshToken: process.env.DROPBOX_REFRESH_TOKEN,
    fetch: fetch
});

// Store browser client tokens
const browserClientTokens = new Map();

// Ensure sync directory exists
fs.ensureDirSync(config.syncDirectory);

// MongoDB Schema
mongoose.connect(process.env.MONGODB_URI);
const FileSchema = new mongoose.Schema({
    path: { type: String, unique: true },
    size: Number,
    lastModified: Date,
    hash: String,
    uploadedBy: String,
    uploaderName: String,
    dropboxPath: String,
    version: { type: Number, default: 1 },
    isLocal: { type: Boolean, default: true } // Track local presence
});
const File = mongoose.model('File', FileSchema);

// Create Express app and HTTP server
const app = express();
const server = http.createServer(app);

// Client management
const activeClients = new Map();
const portAssignments = new Map();
const activeConnections = new Map();

// WebSocket server
const wss = new WebSocket.Server({ server });

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));
app.use('/sync', express.static(config.syncDirectory));

// API endpoints
app.use(express.json());

app.get('/api/auth/status', (req, res) => {
    const clientId = req.query.clientId;
    if (!clientId) {
        return res.status(400).json({ error: 'Client ID required' });
    }
    const isAuthenticated = browserClientTokens.has(clientId);
    res.json({ isAuthenticated });
});

app.get('/api/client/status', (req, res) => {
    const clientId = req.query.clientId;
    if (!clientId) {
        return res.status(400).json({ error: 'Client ID required' });
    }
    const isActive = activeClients.has(clientId);
    res.json({ isActive });
});

app.get('/api/files/download', async (req, res) => {
    const filePath = req.query.path;
    const clientId = req.query.clientId || 'unknown';
    try {
        if (!filePath) {
            return res.status(400).json({ error: 'File path required' });
        }

        const localPath = path.join(config.syncDirectory, filePath);
        if (await fs.pathExists(localPath)) {
            res.download(localPath, path.basename(filePath));
        } else {
            const file = await File.findOne({ path: filePath });
            if (!file) {
                return res.status(404).json({ error: 'File not found' });
            }
            let downloadDbx = dbx;
            if (browserClientTokens.has(clientId)) {
                downloadDbx = new Dropbox({
                    clientId: process.env.DROPBOX_APP_KEY,
                    clientSecret: process.env.DROPBOX_APP_SECRET,
                    refreshToken: browserClientTokens.get(clientId),
                    fetch: fetch
                });
            }
            try {
                const response = await downloadDbx.filesDownload({ path: file.dropboxPath });
                const buffer = Buffer.from(response.result.fileBinary);
                res.set({
                    'Content-Disposition': `attachment; filename="${path.basename(filePath)}"`,
                    'Content-Type': 'application/octet-stream',
                    'Content-Length': buffer.length
                });
                res.send(buffer);
                // Restore to sync_dir
                await fs.ensureDir(path.dirname(localPath));
                await fs.writeFile(localPath, buffer);
                await File.updateOne({ path: filePath }, { isLocal: true });
                broadcastFileChange({
                    eventType: 'update',
                    source: config.clientId,
                    file: { ...file, isLocal: true }
                });
            } catch (err) {
                console.error(`Dropbox download failed for ${filePath}:`, err);
                await File.deleteOne({ path: filePath });
                return res.status(404).json({ error: 'File not found in Dropbox' });
            }
        }
    } catch (err) {
        console.error('Error downloading file:', err);
        res.status(500).json({ error: 'Failed to download file' });
    }
});

// Dropbox OAuth for browser clients
let authResolve = null;
app.get('/auth/dropbox', async (req, res) => {
    try {
        const redirectUri = 'http://localhost:3000/auth/dropbox/callback';
        const authUrl = await dbx.auth.getAuthenticationUrl(redirectUri, null, 'code', 'offline', ['files.content.write', 'files.content.read', 'files.metadata.read']);
        console.log('Opening Dropbox auth URL:', authUrl);

        const authPromise = new Promise((resolve) => {
            authResolve = resolve;
        });

        const open = (await import('open')).default;
        await open(authUrl);

        const token = await authPromise;
        const clientId = req.query.clientId || 'browser_' + crypto.randomUUID();
        browserClientTokens.set(clientId, token);
        console.log(`Stored token for client ${clientId}`);

        res.json({ success: true, clientId });
    } catch (err) {
        console.error('Error starting Dropbox auth:', err);
        res.status(500).json({ error: 'Failed to start Dropbox authentication' });
    }
});

app.get('/auth/dropbox/callback', async (req, res) => {
    try {
        const { code } = req.query;
        if (!code) {
            throw new Error('Authorization code missing');
        }
        console.log('Received authorization code:', code);

        const redirectUri = 'http://localhost:3000/auth/dropbox/callback';
        const tokenResponse = await dbx.auth.getAccessTokenFromCode(redirectUri, code);
        const refreshToken = tokenResponse.result.refresh_token;
        console.log('Obtained refresh token:', refreshToken);

        if (authResolve) {
            authResolve(refreshToken);
            authResolve = null;
        }

        res.send('Authentication successful! You can close this window.');
    } catch (err) {
        console.error('Dropbox auth error:', err);
        res.status(500).send(`Authentication failed: ${err.message}`);
    }
});

// Get all files
app.get('/api/files', async (req, res) => {
    try {
        const files = await File.find().lean();
        res.json(files);
    } catch (err) {
        console.error('Error fetching files:', err);
        res.status(500).json({ error: 'Failed to fetch files' });
    }
});

// Get active clients
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

// Register client
app.post('/api/register', (req, res) => {
    const requestedName = req.body.name || 'Anonymous';
    const remoteAddress = req.ip || req.connection.remoteAddress || req.socket.remoteAddress;
    const parsedIp = remoteAddress ? remoteAddress.replace(/^::ffff:/, '') : 'unknown';

    const newClientId = crypto.randomUUID();

    let assignedPort = null;
    for (let port = config.startPort; port <= config.endPort; port++) {
        if (!portAssignments.has(port)) {
            assignedPort = port;
            break;
        }
    }

    if (assignedPort === null) {
        return res.status(503).json({ error: 'No available ports' });
    }

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

    console.log(`Registered client: ${requestedName} (${newClientId}) at port ${assignedPort}`);

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

// File upload
app.post('/api/files/upload', express.raw({ limit: '100mb', type: '*/*' }), async (req, res) => {
    const filePath = req.query.path;
    const clientId = req.query.clientId || 'unknown';

    try {
        if (!filePath) {
            return res.status(400).json({ error: 'File path required' });
        }

        if (!browserClientTokens.has(clientId)) {
            return res.status(401).json({ error: 'Client must authenticate with Dropbox' });
        }

        if (activeClients.has(clientId)) {
            const client = activeClients.get(clientId);
            client.lastActive = new Date();
            activeClients.set(clientId, client);
        }

        const fullPath = path.join(config.syncDirectory, filePath);
        await fs.ensureDir(path.dirname(fullPath));
        await fs.writeFile(fullPath, req.body);

        const stats = await fs.stat(fullPath);
        const fileHash = crypto.createHash('md5').update(req.body).digest('hex');

        const uploadDbx = new Dropbox({
            clientId: process.env.DROPBOX_APP_KEY,
            clientSecret: process.env.DROPBOX_APP_SECRET,
            refreshToken: browserClientTokens.get(clientId),
            fetch: fetch
        });

        const dropboxPath = `/${filePath}`;
        await uploadDbx.filesUpload({
            path: dropboxPath,
            contents: req.body,
            mode: 'overwrite'
        });

        const uploaderName = activeClients.has(clientId) ? activeClients.get(clientId).name : 'Unknown';

        const existingFile = await File.findOne({ path: filePath });
        let fileInfo = {
            path: filePath,
            size: stats.size,
            lastModified: stats.mtime,
            hash: fileHash,
            uploadedBy: clientId,
            uploaderName: uploaderName,
            dropboxPath: dropboxPath,
            version: 1,
            isLocal: true
        };

        if (existingFile) {
            if (existingFile.hash !== fileHash && existingFile.lastModified > fileInfo.lastModified) {
                fileInfo.version = existingFile.version + 1;
                const conflictPath = `${filePath}.conflict-v${fileInfo.version}`;
                await uploadDbx.filesUpload({
                    path: `/${conflictPath}`,
                    contents: req.body,
                    mode: 'add'
                });
                fileInfo.path = conflictPath;
                fileInfo.dropboxPath = `/${conflictPath}`;
                fileInfo.isLocal = true;

                broadcastFileChange({
                    eventType: 'conflict',
                    source: clientId,
                    file: fileInfo,
                    originalPath: filePath
                });
            } else {
                await File.updateOne({ path: filePath }, fileInfo);
            }
        } else {
            await File.create(fileInfo);
        }

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

// Delete file
app.delete('/api/files', async (req, res) => {
    const filePath = req.query.path;
    const clientId = req.query.clientId || 'unknown';

    try {
        if (!filePath) {
            return res.status(400).json({ error: 'File path required' });
        }

        if (activeClients.has(clientId)) {
            const client = activeClients.get(clientId);
            client.lastActive = new Date();
            activeClients.set(clientId, client);
        }

        let deleteDbx = dbx;
        if (browserClientTokens.has(clientId)) {
            deleteDbx = new Dropbox({
                clientId: process.env.DROPBOX_APP_KEY,
                clientSecret: process.env.DROPBOX_APP_SECRET,
                refreshToken: browserClientTokens.get(clientId),
                fetch: fetch
            });
        }

        const fullPath = path.join(config.syncDirectory, filePath);
        await fs.remove(fullPath);
        const file = await File.findOne({ path: filePath });
        if (file) {
            await deleteDbx.filesDeleteV2({ path: file.dropboxPath });
            await File.deleteOne({ path: filePath });
        }

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

// Heartbeat
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

// Disconnect client
app.post('/api/disconnect', (req, res) => {
    const clientId = req.body.clientId;

    if (!clientId || !activeClients.has(clientId)) {
        return res.status(404).json({ error: 'Client not found' });
    }

    const client = activeClients.get(clientId);

    portAssignments.delete(client.port);
    activeClients.delete(clientId);
    activeConnections.delete(clientId);
    browserClientTokens.delete(clientId);

    console.log(`Client disconnected: ${client.name} (${clientId}) from port ${client.port}`);

    broadcastClientEvent({
        eventType: 'client_disconnected',
        clientId: clientId,
        clientName: client.name
    });

    res.status(200).json({ success: true });
});

// WebSocket handling
wss.on('connection', async (ws, req) => {
    const urlParams = new URLSearchParams(req.url.replace(/^\/|\?.*$/, ''));
    const clientId = urlParams.get('clientId');

    if (!clientId || !activeClients.has(clientId)) {
        console.log(`Invalid WebSocket connection: clientId ${clientId || 'missing'}`);
        ws.close(1008, 'Invalid client ID');
        return;
    }

    console.log(`WebSocket connection from ${req.socket.remoteAddress}, clientId: ${clientId}`);

    ws.clientId = clientId;
    activeConnections.set(clientId, ws);

    if (activeClients.has(clientId)) {
        const client = activeClients.get(clientId);
        client.lastActive = new Date();
        activeClients.set(clientId, client);

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

    const files = await File.find().lean();
    ws.send(JSON.stringify({
        type: 'init',
        clientId,
        files,
        clients: Array.from(activeClients.values()).map(client => ({
            id: client.id,
            name: client.name,
            ipAddress: client.ipAddress,
            port: client.port
        }))
    }));

    ws.on('message', async (message) => {
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
            await handleClientMessage(data, ws);
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

// Handle client messages
async function handleClientMessage(message, ws) {
    switch (message.type) {
        case 'sync_request':
            await sendFileToClient(message.path, ws);
            break;
        case 'change':
            if (message.source !== config.clientId) {
                await processRemoteFileChange(message);
            }
            break;
        case 'conflict_resolve':
            await resolveConflict(message, ws);
            break;
        case 'heartbeat':
            break;
        default:
            console.log('Unknown message type:', message.type);
    }
}

// Send file to client
async function sendFileToClient(filePath, ws) {
    try {
        const file = await File.findOne({ path: filePath });
        if (!file) {
            ws.send(JSON.stringify({ type: 'error', message: 'File not found' }));
            return;
        }
        const downloadDbx = browserClientTokens.has(ws.clientId) ?
            new Dropbox({
                clientId: process.env.DROPBOX_APP_KEY,
                clientSecret: process.env.DROPBOX_APP_SECRET,
                refreshToken: browserClientTokens.get(ws.clientId),
                fetch: fetch
            }) : dbx;
        try {
            const response = await downloadDbx.filesDownload({ path: file.dropboxPath });
            const buffer = Buffer.from(response.result.fileBinary);
            ws.send(JSON.stringify({
                type: 'file_data',
                path: filePath,
                data: buffer.toString('base64')
            }));
            // Restore to sync_dir
            const localPath = path.join(config.syncDirectory, filePath);
            await fs.ensureDir(path.dirname(localPath));
            await fs.writeFile(localPath, buffer);
            await File.updateOne({ path: filePath }, { isLocal: true });
            broadcastFileChange({
                eventType: 'update',
                source: config.clientId,
                file: { ...file, isLocal: true }
            });
        } catch (err) {
            console.error(`Dropbox download failed for ${filePath}:`, err);
            await File.deleteOne({ path: filePath });
            ws.send(JSON.stringify({ type: 'error', message: 'File not found in Dropbox' }));
        }
    } catch (err) {
        console.error('Error sending file:', err);
        ws.send(JSON.stringify({ type: 'error', message: 'Failed to fetch file' }));
    }
}

// Process remote file changes
async function processRemoteFileChange(change) {
    if (change.eventType === 'update') {
        const fileInfo = change.file;
        const existingFile = await File.findOne({ path: fileInfo.path });

        if (existingFile && existingFile.hash !== fileInfo.hash && existingFile.lastModified > fileInfo.lastModified) {
            const conflictPath = `${fileInfo.path}.conflict-v${existingFile.version + 1}`;
            fileInfo.path = conflictPath;
            fileInfo.dropboxPath = `/${conflictPath}`;
            fileInfo.version = existingFile.version + 1;

            const uploadDbx = browserClientTokens.has(change.source) ?
                new Dropbox({
                    clientId: process.env.DROPBOX_APP_KEY,
                    clientSecret: process.env.DROPBOX_APP_SECRET,
                    refreshToken: browserClientTokens.get(change.source),
                    fetch: fetch
                }) : dbx;

            await uploadDbx.filesUpload({
                path: fileInfo.dropboxPath,
                contents: Buffer.from(change.fileContent, 'base64'),
                mode: 'add'
            });

            await File.create(fileInfo);

            broadcastFileChange({
                eventType: 'conflict',
                source: change.source,
                file: fileInfo,
                originalPath: change.file.path
            });
        } else {
            if (existingFile) {
                fileInfo.version = existingFile.version;
                await File.updateOne({ path: fileInfo.path }, fileInfo);
            } else {
                await File.create(fileInfo);
            }
            await fs.ensureDir(path.dirname(path.join(config.syncDirectory, fileInfo.path)));
            await fs.writeFile(path.join(config.syncDirectory, fileInfo.path), Buffer.from(change.fileContent, 'base64'));

            broadcastFileChange({
                eventType: 'update',
                source: change.source,
                file: fileInfo
            });
        }
        console.log(`Remote file updated: ${fileInfo.path}`);
    } else if (change.eventType === 'delete') {
        const file = await File.findOne({ path: change.path });
        if (file) {
            const deleteDbx = browserClientTokens.has(change.source) ?
                new Dropbox({
                    clientId: process.env.DROPBOX_APP_KEY,
                    clientSecret: process.env.DROPBOX_APP_SECRET,
                    refreshToken: browserClientTokens.get(change.source),
                    fetch: fetch
                }) : dbx;
            try {
                await deleteDbx.filesDeleteV2({ path: file.dropboxPath });
                await File.deleteOne({ path: change.path });
            } catch (err) {
                console.warn(`Dropbox deletion failed for ${change.path}, updating local status:`, err);
                await File.updateOne({ path: change.path }, { isLocal: false });
            }
            await fs.remove(path.join(config.syncDirectory, change.path));
            broadcastFileChange({
                eventType: 'delete',
                source: change.source,
                path: change.path
            });
            console.log(`Remote file deleted: ${change.path}`);
        }
    }
}

// Resolve conflicts
async function resolveConflict(message, ws) {
    try {
        const { originalPath, chosenPath } = message;
        const chosenFile = await File.findOne({ path: chosenPath });
        if (!chosenFile) {
            ws.send(JSON.stringify({ type: 'error', message: 'Chosen file not found' }));
            return;
        }

        await File.updateOne({ path: originalPath }, {
            path: originalPath,
            size: chosenFile.size,
            lastModified: new Date(),
            hash: chosenFile.hash,
            uploadedBy: chosenFile.uploadedBy,
            uploaderName: chosenFile.uploaderName,
            dropboxPath: chosenFile.dropboxPath,
            version: chosenFile.version + 1,
            isLocal: chosenFile.isLocal
        });

        const deleteDbx = browserClientTokens.has(message.clientId) ?
            new Dropbox({
                clientId: process.env.DROPBOX_APP_KEY,
                clientSecret: process.env.DROPBOX_APP_SECRET,
                refreshToken: browserClientTokens.get(clientId),
                fetch: fetch
            }) : dbx;
        await deleteDbx.filesDeleteV2({ path: chosenFile.dropboxPath });
        await File.deleteOne({ path: chosenPath });

        broadcastFileChange({
            eventType: 'update',
            source: message.clientId,
            file: await File.findOne({ path: originalPath }).lean()
        });

        ws.send(JSON.stringify({ type: 'conflict_resolved', path: originalPath }));
    } catch (err) {
        console.error('Error resolving conflict:', err);
        ws.send(JSON.stringify({ type: 'error', message: 'Failed to resolve conflict' }));
    }
}

// Broadcast file changes
function broadcastFileChange(change) {
    const message = JSON.stringify({
        type: 'change',
        eventType: change.eventType,
        source: change.source,
        file: change.file,
        path: change.path,
        originalPath: change.originalPath
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

// Broadcast client events
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
    console.log(`Broadcasting client event to ${clientCount} clients: ${event.eventType}`, `sent to: ${sentTo.join(', ')}`);
}

// Sync MongoDB with Dropbox
async function syncMongoWithDropbox() {
    try {
        console.log('Starting MongoDB-Dropbox sync');
        const files = await File.find().lean();
        let deletedCount = 0;
        for (const file of files) {
            try {
                await dbx.filesGetMetadata({ path: file.dropboxPath });
            } catch (err) {
                console.log(`File ${file.path} missing in Dropbox, removing from MongoDB`);
                await File.deleteOne({ path: file.path });
                broadcastFileChange({
                    eventType: 'delete',
                    source: config.clientId,
                    path: file.path
                });
                deletedCount++;
            }
        }
        console.log(`MongoDB-Dropbox sync completed, removed ${deletedCount} stale records`);
    } catch (err) {
        console.error('Error during MongoDB-Dropbox sync:', err);
    }
}

// Check inactive clients
setInterval(() => {
    const now = new Date();
    const inactiveThreshold = 300000;

    activeClients.forEach((client, clientId) => {
        if ((now - client.lastActive) > inactiveThreshold) {
            console.log(`Client inactive, removing: ${client.name} (${clientId})`);
            portAssignments.delete(client.port);
            activeClients.delete(clientId);
            activeConnections.delete(clientId);
            browserClientTokens.delete(clientId);
            broadcastClientEvent({
                eventType: 'client_disconnected',
                clientId: clientId,
                clientName: client.name,
                reason: 'timeout'
            });
        }
    });
}, 60000);

// Periodic MongoDB-Dropbox sync
setInterval(syncMongoWithDropbox, 300000); // Run every 5 minutes

// File system watcher
const watcher = chokidar.watch(config.syncDirectory, {
    ignored: /(^|[\/\\])\../,
    persistent: true,
    ignoreInitial: true,
    awaitWriteFinish: true
});

watcher.on('add', handleFileAdded);
watcher.on('change', handleFileChanged);
watcher.on('unlink', handleFileDeleted);

async function handleFileAdded(filePath) {
    await handleFileChanged(filePath);
}

async function handleFileChanged(filePath) {
    try {
        const relativePath = path.relative(config.syncDirectory, filePath);
        const stats = await fs.stat(filePath);
        const data = await fs.readFile(filePath);
        const fileHash = crypto.createHash('md5').update(data).digest('hex');

        const dropboxPath = `/${relativePath}`;
        await dbx.filesUpload({
            path: dropboxPath,
            contents: data,
            mode: 'overwrite'
        });

        const fileInfo = {
            path: relativePath,
            size: stats.size,
            lastModified: stats.mtime,
            hash: fileHash,
            uploadedBy: config.clientId,
            uploaderName: 'Server',
            dropboxPath: dropboxPath,
            version: 1,
            isLocal: true
        };

        const existingFile = await File.findOne({ path: relativePath });
        if (!existingFile || existingFile.hash !== fileInfo.hash) {
            if (existingFile) {
                fileInfo.version = existingFile.version;
                await File.updateOne({ path: relativePath }, fileInfo);
            } else {
                await File.create(fileInfo);
            }
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

async function handleFileDeleted(filePath) {
    try {
        const relativePath = path.relative(config.syncDirectory, filePath);
        const file = await File.findOne({ path: relativePath });
        if (file) {
            try {
                await dbx.filesGetMetadata({ path: file.dropboxPath });
                // File exists in Dropbox, update isLocal
                await File.updateOne({ path: relativePath }, { isLocal: false });
                broadcastFileChange({
                    eventType: 'update',
                    source: config.clientId,
                    file: { ...file, isLocal: false }
                });
                console.log(`Local file deleted, kept in Dropbox: ${relativePath}`);
            } catch (err) {
                // File missing in Dropbox, delete MongoDB record
                await File.deleteOne({ path: relativePath });
                broadcastFileChange({
                    eventType: 'delete',
                    source: config.clientId,
                    path: relativePath
                });
                console.log(`Local file deleted, removed from MongoDB: ${relativePath}`);
            }
        }
    } catch (err) {
        console.error('Error processing file deletion:', err);
    }
}

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

                const dropboxPath = `/${relativePath}`;
                try {
                    await dbx.filesUpload({
                        path: dropboxPath,
                        contents: data,
                        mode: 'overwrite'
                    });
                } catch (uploadErr) {
                    console.error(`Failed to upload ${relativePath} to Dropbox:`, uploadErr);
                    continue;
                }

                await File.findOneAndUpdate(
                    { path: relativePath },
                    {
                        path: relativePath,
                        size: stats.size,
                        lastModified: stats.mtime,
                        hash: fileHash,
                        uploadedBy: config.clientId,
                        uploaderName: 'Server',
                        dropboxPath: dropboxPath,
                        version: 1,
                        isLocal: true
                    },
                    { upsert: true }
                );
            }
        }

        console.log(`Scanned ${await File.countDocuments()} existing files`);
    } catch (err) {
        console.error('Error scanning existing files:', err);
    }
}

async function init() {
    await mongoose.connection.once('open', () => console.log('Successfully connected to MongoDB'));
    await scanExistingFiles();
    await syncMongoWithDropbox(); // Run sync on startup

    const serverIp = ip.address();
    console.log(serverIp);

    server.listen(config.port, () => {
        console.log(`File sync server running on port ${config.port}`);
        console.log(`Server accessible at http://localhost:${config.port}`);
        console.log(`Sync directory: ${config.syncDirectory}`);
        console.log(`Server ID: ${config.clientId}`);
        console.log(`Client port range: ${config.startPort} - ${config.endPort}`);
    });
}

process.on('SIGINT', async () => {
    console.log('Shutting down server...');
    await mongoose.connection.close();
    server.close(() => {
        console.log('Server shut down successfully');
        process.exit(0);
    });
});

init().catch(err => console.error('Initialization error:', err));