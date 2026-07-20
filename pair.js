const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const {
    default: makeWASocket,
    DisconnectReason,
    fetchLatestBaileysVersion,
    Browsers,
    jidNormalizedUser,
    useMultiFileAuthState,
    makeCacheableSignalKeyStore,
    delay
} = require('@whiskeysockets/baileys');
const P = require('pino');

// ============ LOAD ENV VARIABLES ============
require('dotenv').config();

// ============ CONFIGURATION - EXACTLY LIKE WORKING EXAMPLE ============
const PORT = process.env.PORT || 3000;
const SESSION_DIR = path.join(__dirname, "sessions");
const DATA_DIR = path.join(__dirname, "data");
const TEMP_DIR = path.join(__dirname, "temp");
const BOT_NAME = process.env.BOT_NAME || "404-XMD";
const SESSION_PREFIX = process.env.SESSION_PREFIX || "404_XMD_";
const MAX_USERS = parseInt(process.env.MAX_USERS) || 50;

// MongoDB Configuration - EXACTLY LIKE WORKING EXAMPLE
const SETTINGS_MONGO_URI = process.env.SETTINGS_MONGO_URI || "";
const SETTINGS_MONGO_DB = process.env.SETTINGS_DB_NAME || process.env.SETTINGS_MONGO_DB || "";
const SESSION_MONGO_URI = process.env.MONGO_URI || "mongodb+srv://nuchman254_db_user:254wesongA@cluster0.i00iffy.mongodb.net/?retryWrites=true&w=majority";
const SESSION_MONGO_DB = process.env.MONGO_DB || "xmd_bot";

// ============ ENSURE DIRS ============
[SESSION_DIR, DATA_DIR, TEMP_DIR].forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

process.env.TMPDIR = TEMP_DIR;
process.env.TEMP = TEMP_DIR;
process.env.TMP = TEMP_DIR;

// ============ UTILITY ============
function ensureDirSync(dir) {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
}

// ============ SESSION CACHE (Local-first) - EXACTLY LIKE WORKING EXAMPLE ============
const sessionCache = {
    sessions: new Map(),

    async get(sessionId) {
        // 1. Memory cache
        if (this.sessions.has(sessionId)) {
            return this.sessions.get(sessionId);
        }

        // 2. Local files (primary)
        const sessionPath = path.join(SESSION_DIR, sessionId);
        const credsPath = path.join(sessionPath, 'creds.json');
        if (fs.existsSync(credsPath)) {
            try {
                const creds = JSON.parse(fs.readFileSync(credsPath, 'utf8'));
                const keysPath = path.join(sessionPath, 'keys.json');
                const keys = fs.existsSync(keysPath) ? JSON.parse(fs.readFileSync(keysPath, 'utf8')) : null;
                const sessionData = { creds, keys, isValid: true, isActive: true };
                this.sessions.set(sessionId, sessionData);
                return sessionData;
            } catch (e) {
                // Corrupted file, try MongoDB
                return await this.loadFromMongo(sessionId);
            }
        }

        // 3. MongoDB fallback (only if local missing)
        return await this.loadFromMongo(sessionId);
    },

    async loadFromMongo(sessionId) {
        if (!sessionsCol) return null;
        const sanitized = sessionId.replace(/[^0-9]/g, '');
        try {
            const doc = await sessionsCol.findOne({ number: sanitized });
            if (doc && doc.creds) {
                // Restore to local
                const sessionPath = path.join(SESSION_DIR, sessionId);
                ensureDirSync(sessionPath);
                fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(doc.creds, null, 2));
                if (doc.keys) {
                    fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(doc.keys, null, 2));
                }
                const sessionData = { creds: doc.creds, keys: doc.keys || null, isValid: doc.isValid !== false, isActive: doc.isActive !== false };
                this.sessions.set(sessionId, sessionData);
                return sessionData;
            }
            return null;
        } catch (e) {
            return null;
        }
    },

    // Save to local file immediately
    saveToLocal(sessionId, creds, keys = null) {
        const sessionPath = path.join(SESSION_DIR, sessionId);
        ensureDirSync(sessionPath);
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(creds, null, 2));
        if (keys) {
            fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(keys, null, 2));
        }
        this.sessions.set(sessionId, { creds, keys, isValid: true, isActive: true });
        return true;
    },

    // Queue MongoDB backup (non-blocking)
    queueBackup(sessionId, creds, keys = null) {
        queueSessionSave(sessionId, creds, keys, true, true);
    }
};

// ============ MONGODB CONNECTION - EXACTLY LIKE WORKING EXAMPLE ============
let sessionMongoClient;
let sessionsCol;
let statsCol;
let invalidSessionsCol;

const MONGO_OPTIONS = {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    serverSelectionTimeoutMS: 15000,
    socketTimeoutMS: 30000,
    connectTimeoutMS: 15000,
    maxPoolSize: 5,
    minPoolSize: 2,
    maxIdleTimeMS: 30000,
    heartbeatFrequencyMS: 10000,
    compressors: ['snappy']
};

async function initSessionMongo(retries = 3) {
    for (let i = 0; i < retries; i++) {
        try {
            if (sessionMongoClient?.topology?.isConnected) return true;
            sessionMongoClient = new MongoClient(SESSION_MONGO_URI, MONGO_OPTIONS);
            await sessionMongoClient.connect();
            const db = sessionMongoClient.db(SESSION_MONGO_DB);
            sessionsCol = db.collection('sessions');
            statsCol = db.collection('bot_stats');
            invalidSessionsCol = db.collection('invalid_sessions');
            await db.command({ ping: 1 });
            // Create indexes in background
            Promise.all([
                sessionsCol.createIndex({ number: 1 }, { unique: true, background: true }),
                sessionsCol.createIndex({ updatedAt: -1 }, { background: true }),
                sessionsCol.createIndex({ isValid: 1 }, { background: true }),
                sessionsCol.createIndex({ isActive: 1 }, { background: true }),
                sessionsCol.createIndex({ sessionId: 1 }, { background: true }),
                invalidSessionsCol.createIndex({ number: 1 }, { unique: true, background: true }),
                invalidSessionsCol.createIndex({ loggedOutAt: -1 }, { background: true }),
                statsCol.createIndex({ timestamp: -1 }, { background: true })
            ]).catch(() => {});
            console.log(`✅ Session DB (${SESSION_MONGO_DB}) connected`);
            return true;
        } catch (error) {
            console.log(`⚠️ Session DB attempt ${i+1}/${retries} failed: ${error.message}`);
            if (i === retries - 1) return false;
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }
    return false;
}

async function initMongo() {
    return await initSessionMongo();
}

// ============ BATCH SAVES (non-blocking) - EXACTLY LIKE WORKING EXAMPLE ============
let pendingSaves = [];
let saveTimeout = null;

async function batchSaveToMongo() {
    if (pendingSaves.length === 0) return;
    const batch = pendingSaves.slice();
    pendingSaves = [];
    try {
        const operations = batch.map(({ number, creds, keys, isValid, isActive, sessionId }) => ({
            updateOne: {
                filter: { number: number.replace(/[^0-9]/g, '') },
                update: {
                    $set: {
                        creds, keys, isValid, isActive,
                        sessionId: sessionId || null,
                        updatedAt: new Date(),
                        lastBackup: new Date()
                    }
                },
                upsert: true
            }
        }));
        if (sessionsCol && operations.length > 0) {
            await sessionsCol.bulkWrite(operations, { ordered: false });
        }
    } catch (e) {
        // Fallback
        for (const item of batch) {
            await saveSessionToMongo(item.number, item.creds, item.keys, item.isValid, item.isActive, item.sessionId);
        }
    }
}

function queueSessionSave(number, creds, keys = null, isValid = true, isActive = true, sessionId = null) {
    pendingSaves.push({ number, creds, keys, isValid, isActive, sessionId });
    if (!saveTimeout) {
        saveTimeout = setTimeout(() => {
            saveTimeout = null;
            batchSaveToMongo();
        }, 1000);
    }
}

// ============ SESSION FUNCTIONS - EXACTLY LIKE WORKING EXAMPLE ============
async function saveSessionToMongo(number, creds, keys = null, isValid = true, isActive = true, sessionId = null) {
    sessionCache.saveToLocal(number, creds, keys);
    sessionCache.queueBackup(number, creds, keys, isValid, isActive, sessionId);
    return true;
}

async function updateSessionActiveStatus(number, isActive) {
    const sessionData = await sessionCache.get(number);
    if (sessionData) {
        sessionData.isActive = isActive;
        sessionCache.sessions.set(number, sessionData);
        const sanitized = number.replace(/[^0-9]/g, '');
        if (sessionsCol) {
            await sessionsCol.updateOne(
                { number: sanitized },
                { $set: { isActive, updatedAt: new Date() } }
            );
        }
    }
    return true;
}

async function deleteSessionCompletely(sessionId, sessionPath) {
    const sanitized = sessionId.replace(/[^0-9]/g, '');
    sessionCache.sessions.delete(sessionId);
    if (sessionsCol) {
        await sessionsCol.deleteOne({ number: sanitized });
    }
    if (fs.existsSync(sessionPath)) {
        fs.rmSync(sessionPath, { recursive: true, force: true });
    }
    return true;
}

async function saveFullSessionToMongo(sessionId, sessionPath) {
    try {
        const credsPath = path.join(sessionPath, 'creds.json');
        if (fs.existsSync(credsPath)) {
            const credsObj = JSON.parse(fs.readFileSync(credsPath, 'utf8'));
            const keysPath = path.join(sessionPath, 'keys.json');
            const keysObj = fs.existsSync(keysPath) ? JSON.parse(fs.readFileSync(keysPath, 'utf8')) : null;
            sessionCache.saveToLocal(sessionId, credsObj, keysObj);
            sessionCache.queueBackup(sessionId, credsObj, keysObj);
        }
    } catch (error) {}
}

// ============ GENERATE SESSION ID - UPDATED ============
function generateSessionId() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let result = '';
    for (let i = 0; i < 5; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return `${SESSION_PREFIX}${result}`;
}

async function getUniqueSessionId() {
    let sessionId;
    let exists = true;
    let attempts = 0;

    while (exists && attempts < 10) {
        sessionId = generateSessionId();
        attempts++;
        if (sessionsCol) {
            const existing = await sessionsCol.findOne({ sessionId });
            exists = !!existing;
        } else {
            exists = false;
        }
    }
    return sessionId;
}

// ============ ACTIVE SESSIONS TRACKING - EXACTLY LIKE WORKING EXAMPLE ============
let activeSessions = 0;
let totalUsers = 0;
const activeConnections = new Map();
const reconnectAttempts = new Map();
const MAX_RECONNECT_ATTEMPTS = 10;

function canAddNewUser() {
    return activeSessions < MAX_USERS;
}

function getServerCapacity() {
    if (MAX_USERS === 0) return 0;
    return Math.min(100, Math.max(0, Math.round((activeSessions / MAX_USERS) * 100)));
}

// ============ PERSISTENT DATA - EXACTLY LIKE WORKING EXAMPLE ============
const DATA_FILE = path.join(DATA_DIR, 'persistent-data.json');

function loadPersistentData() {
    try {
        if (fs.existsSync(DATA_FILE)) {
            const data = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
            totalUsers = data.totalUsers || 0;
            activeSessions = data.activeSessions || 0;
        }
    } catch (error) {
        totalUsers = 0;
        activeSessions = 0;
    }
}

function savePersistentData() {
    try {
        fs.writeFileSync(DATA_FILE, JSON.stringify({
            totalUsers,
            activeSessions,
            lastUpdated: new Date().toISOString()
        }, null, 2));
    } catch (error) {}
}

loadPersistentData();
setInterval(() => savePersistentData(), 60000);

// ============ EXPRESS APP ============
const app = express();
app.use(express.json({ limit: '10mb' }));
app.use(cors());
app.use(express.static('public'));

// ============ API ENDPOINTS - EXACTLY LIKE WORKING EXAMPLE ============
app.get("/health", (req, res) => {
    res.json({
        status: "healthy",
        timestamp: Date.now(),
        activeSessions,
        totalUsers,
        maxUsers: MAX_USERS,
        capacity: getServerCapacity()
    });
});

app.get("/api/status", (req, res) => {
    res.json({
        activeUsers: activeSessions,
        maxUsers: MAX_USERS,
        available: canAddNewUser(),
        capacity: getServerCapacity()
    });
});

// ============ PAIRING API - EXACTLY LIKE WORKING EXAMPLE ============
app.post("/api/pair", async (req, res) => {
    let conn;
    try {
        const { number } = req.body;
        if (!number) {
            return res.status(400).json({ error: "Phone number is required" });
        }

        const normalizedNumber = number.replace(/\D/g, "");

        // Check capacity - EXACTLY LIKE WORKING EXAMPLE
        if (!canAddNewUser()) {
            return res.status(429).json({
                success: false,
                error: "SERVER AT MAXIMUM CAPACITY",
                message: `Maximum ${MAX_USERS} users reached.`,
                activeUsers: activeSessions,
                maxUsers: MAX_USERS,
                capacity: getServerCapacity()
            });
        }

        const sessionPath = path.join(SESSION_DIR, normalizedNumber);

        // Try to load session from cache/local first - EXACTLY LIKE WORKING EXAMPLE
        const sessionData = await sessionCache.get(normalizedNumber);
        if (sessionData && sessionData.creds) {
            console.log(`📂 Found existing session for ${normalizedNumber}`);
            ensureDirSync(sessionPath);
            fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(sessionData.creds, null, 2));
            if (sessionData.keys) {
                fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(sessionData.keys, null, 2));
            }
        }

        const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
        const { version } = await fetchLatestBaileysVersion();

        conn = makeWASocket({
            logger: P({ level: "silent" }),
            printQRInTerminal: false,
            auth: state,
            version,
            browser: Browsers.macOS("Edge"),
            connectTimeoutMs: 30000,
            keepAliveIntervalMs: 30000,
            maxIdleTimeMs: 60000,
            maxRetries: 3,
            markOnlineOnConnect: true,
            emitOwnEvents: true,
            defaultQueryTimeoutMs: 30000,
            syncFullHistory: false,
            transactionOpts: {
                maxCommitRetries: 3,
                delayBetweenTriesMs: 3000
            }
        });

        // Creds update handler - EXACTLY LIKE WORKING EXAMPLE
        conn.ev.on('creds.update', async () => {
            try {
                await saveCreds();
                const credsPath = path.join(sessionPath, 'creds.json');
                if (fs.existsSync(credsPath)) {
                    const credsObj = JSON.parse(fs.readFileSync(credsPath, 'utf8'));
                    const keysPath = path.join(sessionPath, 'keys.json');
                    const keysObj = fs.existsSync(keysPath) ? JSON.parse(fs.readFileSync(keysPath, 'utf8')) : null;
                    sessionCache.saveToLocal(normalizedNumber, credsObj, keysObj);
                    sessionCache.queueBackup(normalizedNumber, credsObj, keysObj);
                }
            } catch (err) {}
        });

        // Store connection and setup handlers - EXACTLY LIKE WORKING EXAMPLE
        activeConnections.set(normalizedNumber, { conn, saveCreds, lastSeen: Date.now() });
        setupConnectionHandlers(conn, normalizedNumber, saveCreds, sessionPath);

        // Generate pairing code
        await new Promise(resolve => setTimeout(resolve, 2000));
        const pairingCode = await conn.requestPairingCode(normalizedNumber);

        // Format code for display
        const formattedCode = pairingCode.match(/.{1,4}/g).join(' ');

        // Send response - EXACTLY LIKE WORKING EXAMPLE
        res.json({
            success: true,
            pairingCode: pairingCode,
            formattedCode: formattedCode,
            message: "Pairing code generated successfully. Enter it on WhatsApp to complete pairing.",
            activeUsers: activeSessions,
            maxUsers: MAX_USERS,
            capacity: getServerCapacity()
        });

    } catch (error) {
        console.error(`❌ Pairing error: ${error.message}`);
        if (conn) try { conn.ws.close(); } catch (e) {}
        res.status(500).json({
            success: false,
            error: "Failed to generate pairing code",
            details: error.message
        });
    }
});

// ============ CONNECTION HANDLERS - UPDATED: DISCONNECT AFTER SENDING SESSION ============
function setupConnectionHandlers(conn, sessionId, saveCreds, sessionPath) {
    let hasShownConnectedMessage = false;
    let hasConnected = false;
    let heartbeatInterval = null;
    let hasCompleted = false; // flag to prevent reconnection after pairing done

    conn.ev.on("connection.update", async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === "open") {
            reconnectAttempts.set(sessionId, 0);

            if (!hasConnected) {
                hasConnected = true;
                activeSessions++;
                totalUsers++;
                savePersistentData();
                updateSessionActiveStatus(sessionId, true);
                saveFullSessionToMongo(sessionId, sessionPath);
            }

            if (!hasShownConnectedMessage) {
                hasShownConnectedMessage = true;
                // Start a short heartbeat for message delivery
                heartbeatInterval = setInterval(async () => {
                    try {
                        if (conn?.user) {
                            await conn.sendPresenceUpdate('available');
                        }
                    } catch (error) {}
                }, 15000);

                setTimeout(async () => {
                    try {
                        // Generate unique session ID
                        const uniqueId = await getUniqueSessionId();
                        const userJid = `${conn.user.id.split(":")[0]}@s.whatsapp.net`;

                        // Save session to MongoDB with the generated ID
                        const credsPath = path.join(sessionPath, 'creds.json');
                        if (fs.existsSync(credsPath)) {
                            const credsObj = JSON.parse(fs.readFileSync(credsPath, 'utf8'));
                            const keysPath = path.join(sessionPath, 'keys.json');
                            const keysObj = fs.existsSync(keysPath) ? JSON.parse(fs.readFileSync(keysPath, 'utf8')) : null;
                            await saveSessionToMongo(sessionId, credsObj, keysObj, true, true, uniqueId);
                            if (sessionsCol) {
                                await sessionsCol.updateOne(
                                    { number: sessionId },
                                    { $set: { sessionId: uniqueId, pairedNumber: sessionId } }
                                );
                            }
                        }

                        // Send session ID message
                        await conn.sendMessage(userJid, {
                            text: `✅ *PAIRED SUCCESSFULLY!*\n\n` +
                                  `🤖 *Bot:* ${BOT_NAME}\n` +
                                  `📱 *Your Number:* ${sessionId}\n` +
                                  `🔑 *Session ID:* ${uniqueId}\n\n` +
                                  `📋 *How to use this session:*\n` +
                                  `1️⃣ Copy this Session ID: *${uniqueId}*\n` +
                                  `2️⃣ In your settings.js, add:\n` +
                                  `   \`SESSION_ID = "${uniqueId}"\`\n` +
                                  `3️⃣ Restart your bot\n\n` +
                                  `⚠️ *IMPORTANT:*\n` +
                                  `• Keep this Session ID SECRET\n` +
                                  `• Never share it with anyone\n` +
                                  `• Active users: ${activeSessions}/${MAX_USERS}\n\n` +
                                  `💡 *Need help?* Contact the bot owner.`
                        });
                        console.log(`📨 Session ID sent to ${sessionId}: ${uniqueId}`);

                        await delay(1000);
                        await conn.sendMessage(userJid, {
                            text: `📋 *Your Session ID (copy this):*\n\n` +
                                  `\`\`\`${uniqueId}\`\`\`\n\n` +
                                  `_This ID is used to identify your bot session._`
                        });

                        // Mark as completed to prevent reconnection
                        hasCompleted = true;

                        // Disconnect cleanly after a short delay
                        await delay(2000);
                        console.log(`🔌 Disconnecting session ${sessionId} after successful pairing`);
                        if (heartbeatInterval) clearInterval(heartbeatInterval);
                        activeConnections.delete(sessionId);
                        activeSessions = Math.max(0, activeSessions - 1);
                        savePersistentData();
                        updateSessionActiveStatus(sessionId, false);
                        try { conn.ws.close(); } catch (e) {}

                    } catch (error) {
                        console.error('Error sending session message:', error);
                        // Fallback message
                        try {
                            const userJid = `${conn.user.id.split(":")[0]}@s.whatsapp.net`;
                            const simpleId = await getUniqueSessionId();
                            await conn.sendMessage(userJid, {
                                text: `✅ *PAIRED!*\n\n` +
                                      `🔑 *Session ID:* ${simpleId}\n\n` +
                                      `Add this to your settings.js:\n` +
                                      `\`SESSION_ID = "${simpleId}"\``
                            });
                            hasCompleted = true;
                            await delay(2000);
                            if (heartbeatInterval) clearInterval(heartbeatInterval);
                            activeConnections.delete(sessionId);
                            activeSessions = Math.max(0, activeSessions - 1);
                            savePersistentData();
                            updateSessionActiveStatus(sessionId, false);
                            try { conn.ws.close(); } catch (e) {}
                        } catch (e) {
                            console.error('Failed to send even simple message:', e);
                        }
                    }
                }, 2000);
            }
        }

        if (connection === "close") {
            if (heartbeatInterval) {
                clearInterval(heartbeatInterval);
                heartbeatInterval = null;
            }
            // If the session has completed its job, do not reconnect.
            if (hasCompleted) {
                console.log(`🔒 Session ${sessionId} completed, ignoring close event.`);
                activeConnections.delete(sessionId);
                return;
            }

            if (hasConnected) {
                hasConnected = false;
                activeSessions = Math.max(0, activeSessions - 1);
                savePersistentData();
                updateSessionActiveStatus(sessionId, false);
            }

            const isLoggedOut = lastDisconnect?.error?.output?.statusCode === DisconnectReason.loggedOut;
            if (isLoggedOut) {
                console.log(`🔄 Session ${sessionId} logged out, deleting...`);
                await deleteSessionCompletely(sessionId, sessionPath);
                activeConnections.delete(sessionId);
                savePersistentData();
                return;
            }

            // Auto-reconnect for incomplete sessions only
            const currentAttempts = reconnectAttempts.get(sessionId) || 0;
            if (currentAttempts < MAX_RECONNECT_ATTEMPTS && activeConnections.has(sessionId)) {
                reconnectAttempts.set(sessionId, currentAttempts + 1);
                const delayTime = Math.min(2000 * Math.pow(1.3, currentAttempts), 30000);
                console.log(`🔄 Reconnecting ${sessionId} (attempt ${currentAttempts + 1}/${MAX_RECONNECT_ATTEMPTS}) in ${delayTime/1000}s...`);
                setTimeout(() => {
                    if (activeConnections.has(sessionId) && !hasCompleted) {
                        initializeConnection(sessionId);
                    }
                }, delayTime);
            } else if (currentAttempts >= MAX_RECONNECT_ATTEMPTS) {
                console.log(`❌ Max reconnection attempts reached for ${sessionId}`);
                activeConnections.delete(sessionId);
            }
        }
    });

    conn.ev.on("creds.update", async () => {
        if (saveCreds) {
            await saveCreds();
            saveFullSessionToMongo(sessionId, sessionPath);
        }
    });
}

// ============ REINITIALIZE CONNECTION - EXACTLY LIKE WORKING EXAMPLE ============
async function initializeConnection(sessionId) {
    try {
        const sessionPath = path.join(SESSION_DIR, sessionId);
        let needsRestore = false;
        if (!fs.existsSync(sessionPath) || !fs.existsSync(path.join(sessionPath, 'creds.json'))) {
            needsRestore = true;
        }
        if (needsRestore) {
            const mongoSession = await sessionCache.loadFromMongo(sessionId);
            if (!mongoSession) return;
        }
        const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
        const { version } = await fetchLatestBaileysVersion();
        const conn = makeWASocket({
            logger: P({ level: "silent" }),
            printQRInTerminal: false,
            auth: state,
            version,
            browser: Browsers.macOS("Edge"),
            connectTimeoutMs: 30000,
            keepAliveIntervalMs: 30000,
            maxIdleTimeMs: 60000,
            maxRetries: 3,
            markOnlineOnConnect: true,
            emitOwnEvents: true,
            defaultQueryTimeoutMs: 30000,
            syncFullHistory: false
        });
        if (!activeConnections.has(sessionId)) {
            activeConnections.set(sessionId, { conn, saveCreds, lastSeen: Date.now() });
        }
        setupConnectionHandlers(conn, sessionId, saveCreds, sessionPath);
        conn.ev.on('creds.update', async () => {
            await saveFullSessionToMongo(sessionId, sessionPath);
        });
    } catch (error) {
        console.error(`Error initializing connection for ${sessionId}:`, error);
    }
}

// ============ RELOAD EXISTING SESSIONS - DISABLED ============
// No longer reloads sessions on startup to match the pairing-only behaviour.
async function reloadExistingSessions() {
    // Intentionally empty: sessions will only be created via pairing API.
    console.log(`⏭️ Skipping session reload (pairing-only mode).`);
}

// ============ START SERVER ============
const server = app.listen(PORT, '0.0.0.0', async () => {
    console.log(`
╔═══════════════════════════════════════╗
║   📱 WHATSAPP PAIRING APP            ║
║   Port: ${PORT}                         ║
║   Session DB: ${SESSION_MONGO_DB}          ║
║   Bot: ${BOT_NAME}                       ║
║   Max Users: ${MAX_USERS}               ║
║   Session Prefix: ${SESSION_PREFIX}       ║
╚═══════════════════════════════════════╝
    `);
    await initMongo(); // still connect to MongoDB for saving sessions
    await reloadExistingSessions(); // won't actually load anything now
});

// ============ GRACEFUL SHUTDOWN - EXACTLY LIKE WORKING EXAMPLE ============
let isShuttingDown = false;

async function gracefulShutdown() {
    if (isShuttingDown) return;
    isShuttingDown = true;

    console.log("\n🛑 Shutting down...");
    savePersistentData();

    if (saveTimeout) {
        clearTimeout(saveTimeout);
        await batchSaveToMongo();
    }

    for (const [sessionId, data] of activeConnections) {
        try {
            const sessionPath = path.join(SESSION_DIR, sessionId);
            await saveFullSessionToMongo(sessionId, sessionPath);
        } catch (err) {}
        try {
            if (data.conn && typeof data.conn.ws.close === 'function') {
                data.conn.ws.close();
            }
        } catch (error) {}
    }

    if (sessionMongoClient) await sessionMongoClient.close();

    server.close(async () => {
        process.exit(0);
    });
}

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);
process.on("uncaughtException", (error) => {
    console.error('Uncaught Exception:', error);
});
process.on("unhandledRejection", (reason, promise) => {
    console.error('Unhandled Rejection:', reason);
});

module.exports = { activeConnections, activeSessions, totalUsers };