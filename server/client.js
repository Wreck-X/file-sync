const WebSocket = require('ws');
const fs = require('fs-extra');
const path = require('path');
const chokidar = require('chokidar');
const crypto = require('crypto');
const axios = require('axios');

const config = {
    serverUrl: 'http://localhost:3000',
    wsUrl: 'ws://localhost:3000',
    syncDirectory: path.join(__dirname, 'client_sync_dir'),
    clientName: null,
    clientId: null,
};

async function registerClient() {
    try {
        config.clientName = process.argv[2] || 'DesktopClient';
        await fs.ensureDir(config.syncDirectory);
        
        const response = await axios.post(`${config.serverUrl}/api/register`, {
            name: config.clientName
        });
        config.clientId = response.data.clientId;
        console.log(`Registered client: ${config.clientName} (${config.clientId})`);
        initWebSocket();
    } catch (err) {
        console.error('Registration error:', err.message);
        process.exit(1);
    }
}

function initWebSocket() {
    const wsUrl = new URL(config.wsUrl);
    wsUrl.searchParams.append('clientId', config.clientId);
    wsUrl.searchParams.append('uniqueId', Date.now().toString());
    const ws = new WebSocket(wsUrl);
    
    ws.on('open', () => {
        console.log(`WebSocket connected for client ${config.clientId}`);
        setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'heartbeat' }));
                console.log('Sent heartbeat');
            }
        }, 60000);
    });
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message.toString());
            await handleMessage(data, ws);
        } catch (err) {
            console.error('Error processing WebSocket message:', err);
        }
    });
    
    ws.on('close', () => {
        console.log('WebSocket closed, reconnecting in 10 seconds...');
        setTimeout(initWebSocket, 10000);
    });
    
    ws.on('error', (err) => {
        console.error('WebSocket error:', err);
    });
    
    return ws;
}

async function handleMessage(data, ws) {
    switch (data.type) {
        case 'init':
            console.log('Received initial sync data, files:', data.files.length);
            for (const file of data.files) {
                const localPath = path.join(config.syncDirectory, file.path);
                if (!await fs.exists(localPath)) {
                    ws.send(JSON.stringify({ type: 'sync_request', path: file.path }));
                    console.log(`Requested sync for ${file.path}`);
                }
            }
            break;
        case 'change':
            if (data.eventType === 'update' && data.file && data.file.path) {
                ws.send(JSON.stringify({ type: 'sync_request', path: data.file.path }));
                console.log(`Requested update for ${data.file.path}`);
            } else if (data.eventType === 'delete' && data.path) {
                const localPath = path.join(config.syncDirectory, data.path);
                try {
                    await fs.remove(localPath);
                    console.log(`Deleted local file: ${data.path}`);
                } catch (err) {
                    console.error(`Error deleting ${data.path}:`, err);
                }
            } else if (data.eventType === 'conflict') {
                console.log(`Conflict detected for ${data.originalPath}: ${data.file.path} (v${data.file.version})`);
            }
            break;
        case 'file_data':
            try {
                const filePath = path.join(config.syncDirectory, data.path);
                await fs.ensureDir(path.dirname(filePath));
                await fs.writeFile(filePath, Buffer.from(data.data, 'base64'));
                console.log(`Synced file: ${data.path}`);
            } catch (err) {
                console.error(`Error syncing ${data.path}:`, err);
            }
            break;
        case 'error':
            console.error('Server error:', data.message);
            break;
        case 'conflict_resolved':
            console.log(`Conflict resolved for ${data.path}`);
            break;
        case 'heartbeat_ack':
            console.log('Heartbeat acknowledged:', data.timestamp);
            break;
        default:
            console.warn('Unknown message type:', data.type);
    }
}

// File system watcher
const watcher = chokidar.watch(config.syncDirectory, {
    ignored: /(^|[\/\\])\../,
    persistent: true,
    ignoreInitial: false,
    awaitWriteFinish: {
        stabilityThreshold: 1000,
        pollInterval: 100
    }
});

watcher.on('add', async (filePath) => {
    console.log(`Detected new file: ${filePath}`);
    await handleFileChange(filePath);
});

watcher.on('change', async (filePath) => {
    console.log(`Detected file change: ${filePath}`);
    await handleFileChange(filePath);
});

watcher.on('unlink', async (filePath) => {
    try {
        const relativePath = path.relative(config.syncDirectory, filePath);
        await axios.delete(`${config.serverUrl}/api/files?path=${encodeURIComponent(relativePath)}&clientId=${encodeURIComponent(config.clientId)}`);
        console.log(`Notified server of deletion: ${relativePath}`);
    } catch (err) {
        console.error(`Error notifying deletion of ${filePath}:`, err);
    }
});

async function handleFileChange(filePath) {
    try {
        const relativePath = path.relative(config.syncDirectory, filePath);
        const data = await fs.readFile(filePath);
        const fileHash = crypto.createHash('md5').update(data).digest('hex');
        
        const response = await axios.post(
            `${config.serverUrl}/api/files/upload?path=${encodeURIComponent(relativePath)}&clientId=${encodeURIComponent(config.clientId)}`,
            data,
            { headers: { 'Content-Type': 'application/octet-stream' } }
        );
        console.log(`Uploaded file to server: ${relativePath}, hash: ${fileHash}`);
    } catch (err) {
        console.error(`Error processing file ${filePath}:`, err);
    }
}

registerClient().catch(err => {
    console.error('Client initialization failed:', err);
    process.exit(1);
});