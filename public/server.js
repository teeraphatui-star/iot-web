const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const admin = require('firebase-admin');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static('public'));

// ===== ตั้งค่าผ่าน Environment Variables บน Render =====
// API_KEY                  : รหัสลับที่ ESP32 ต้องส่งมาใน header x-api-key (ถ้าไม่ตั้ง = ไม่ตรวจ)
// FIREBASE_SERVICE_ACCOUNT : เนื้อไฟล์ JSON ของ service account ทั้งก้อน
const API_KEY = process.env.API_KEY;
const FIELDS = ['temperature', 'humidity', 'pm25', 'smoke'];

let db = null;
try {
    if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        admin.initializeApp({
            credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT))
        });
        db = admin.firestore();
        console.log('Firestore connected');
    } else {
        console.warn('FIREBASE_SERVICE_ACCOUNT not set: history is disabled');
    }
} catch (e) {
    console.error('Firebase init failed:', e.message);
}

let latest = null;  // ค่าล่าสุดที่ ESP32 ส่งมา
let lastSeen = 0;   // เวลา (ms) ที่ได้รับข้อมูลล่าสุด
let bucket = null;  // ตัวสะสมค่าเฉลี่ยของนาทีปัจจุบัน

// บันทึกค่าเฉลี่ยของ 1 นาทีลง Firestore (1 เอกสารต่อนาที ประหยัดโควตาฟรี)
async function flush(b) {
    if (!db || !b) return;
    const doc = { ts: b.key, n: b.n };
    FIELDS.forEach(k => { doc[k] = +(b.sum[k] / b.n).toFixed(2); });
    try {
        await db.collection('readings').doc(String(b.key)).set(doc);
    } catch (e) {
        console.error('Firestore write failed:', e.message);
    }
}

// รับข้อมูลจาก ESP32
app.post('/update', (req, res) => {
    if (API_KEY && req.get('x-api-key') !== API_KEY) {
        return res.status(401).json({ error: 'invalid api key' });
    }
    const d = req.body;
    if (!d || !FIELDS.every(k => typeof d[k] === 'number' && isFinite(d[k]))) {
        return res.status(400).json({ error: 'bad payload' });
    }

    latest = { temperature: d.temperature, humidity: d.humidity, pm25: d.pm25, smoke: d.smoke, ts: Date.now() };
    lastSeen = latest.ts;
    io.emit('sensor_update', latest); // ส่งให้หน้าเว็บแบบเรียลไทม์

    const key = Math.floor(latest.ts / 60000) * 60000;
    if (!bucket || bucket.key !== key) {
        flush(bucket); // ขึ้นนาทีใหม่ -> บันทึกนาทีที่แล้ว
        bucket = { key, n: 0, sum: { temperature: 0, humidity: 0, pm25: 0, smoke: 0 } };
    }
    bucket.n++;
    FIELDS.forEach(k => { bucket.sum[k] += d[k]; });

    res.sendStatus(200);
});

// ===== REST API =====
const RANGES = { '1h': 3600e3, '6h': 21600e3, '24h': 86400e3 };
const cache = {}; // แคช 30 วินาที ลดจำนวนการอ่าน Firestore

async function getHistory(range) {
    const r = RANGES[range] ? range : '1h';
    if (!db) return [];
    if (cache[r] && Date.now() - cache[r].at < 30000) return cache[r].rows;
    const snap = await db.collection('readings')
        .where('ts', '>=', Date.now() - RANGES[r])
        .orderBy('ts').limit(1500).get();
    const rows = snap.docs.map(d => d.data());
    cache[r] = { at: Date.now(), rows };
    return rows;
}

const wrap = fn => async (req, res) => {
    try { await fn(req, res); } catch (e) { res.status(500).json({ error: e.message }); }
};

// ค่าล่าสุด
app.get('/api/latest', (req, res) => res.json({ latest, lastSeen }));

// ประวัติย้อนหลัง ?range=1h|6h|24h
app.get('/api/history', wrap(async (req, res) => res.json(await getHistory(req.query.range))));

// สถิติ min / avg / max ?range=1h|6h|24h
app.get('/api/stats', wrap(async (req, res) => {
    const rows = await getHistory(req.query.range);
    const out = { count: rows.length };
    FIELDS.forEach(k => {
        const v = rows.map(r => r[k]);
        out[k] = v.length
            ? { min: Math.min(...v), avg: +(v.reduce((a, b) => a + b, 0) / v.length).toFixed(2), max: Math.max(...v) }
            : null;
    });
    res.json(out);
}));

// ดาวน์โหลด CSV ?range=1h|6h|24h
app.get('/api/export.csv', wrap(async (req, res) => {
    const rows = await getHistory(req.query.range);
    const csv = ['time,temperature,humidity,pm25,smoke']
        .concat(rows.map(r => [new Date(r.ts).toISOString(), r.temperature, r.humidity, r.pm25, r.smoke].join(',')))
        .join('\n');
    res.set({ 'Content-Type': 'text/csv; charset=utf-8', 'Content-Disposition': 'attachment; filename="pc-monitor.csv"' });
    res.send(csv);
}));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server is running on port ${PORT}`);
});
