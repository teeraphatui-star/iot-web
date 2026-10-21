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

// ===== สรุปรายวัน (เวลาไทย) เก็บใน Firestore: daily/{YYYY-MM-DD} =====
const SMOKE_LIMIT = 80;                        // ให้ตรงกับเฟิร์มแวร์
const DUST_CUT = [15, 25, 37.5, 75];           // เกณฑ์ฝุ่น 5 ระดับ (ตรงกับเฟิร์มแวร์)
const thai = ts => new Date(ts + 7 * 3600e3);  // เลื่อนเป็นเวลาไทย แล้วอ่านด้วย getUTC*
const dayKey = ts => thai(ts).toISOString().slice(0, 10);
const emptyDay = key => ({ date: key, n: 0, tmin: null, tmax: null, pmin: null, pmax: null,
    psum: 0, smax: 0, sover: 0, levels: [0, 0, 0, 0, 0], hours: {} });
let today = emptyDay(dayKey(Date.now()));

async function saveDaily() {
    if (!db) return;
    try { await db.collection('daily').doc(today.date).set(today); }
    catch (e) { console.error('Daily write failed:', e.message); }
}
// เซิร์ฟเวอร์รีสตาร์ทกลางวัน -> โหลดสรุปของวันนี้กลับมาต่อ
if (db) db.collection('daily').doc(today.date).get()
    .then(s => { if (s.exists && today.n === 0) today = s.data(); })
    .catch(e => console.error('Daily load failed:', e.message));

function addDaily(d, ts) {
    const key = dayKey(ts);
    if (key !== today.date) { saveDaily(); today = emptyDay(key); } // ขึ้นวันใหม่
    const lo = (a, v) => a === null ? v : Math.min(a, v);
    const hi = (a, v) => a === null ? v : Math.max(a, v);
    today.n++;
    today.tmin = lo(today.tmin, d.temperature); today.tmax = hi(today.tmax, d.temperature);
    today.pmin = lo(today.pmin, d.pm25);        today.pmax = hi(today.pmax, d.pm25);
    today.psum += d.pm25;
    today.smax = Math.max(today.smax, d.smoke);
    if (d.smoke > SMOKE_LIMIT) today.sover++;
    today.levels[DUST_CUT.filter(c => d.pm25 > c).length]++;
    const h = String(thai(ts).getUTCHours());
    const o = today.hours[h] || (today.hours[h] = { n: 0, tmin: null, tmax: null, psum: 0, pmax: 0, smax: 0 });
    o.n++; o.tmin = lo(o.tmin, d.temperature); o.tmax = hi(o.tmax, d.temperature);
    o.psum += d.pm25; o.pmax = Math.max(o.pmax, d.pm25); o.smax = Math.max(o.smax, d.smoke);
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
    addDaily(d, latest.ts);           // สะสมสรุปรายวัน

    const key = Math.floor(latest.ts / 60000) * 60000;
    if (!bucket || bucket.key !== key) {
        flush(bucket); // ขึ้นนาทีใหม่ -> บันทึกนาทีที่แล้ว
        saveDaily();   // บันทึกสรุปรายวันลง Firestore (1 เขียน/นาที)
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

// ===== สรุปรายวัน =====
const view = s => ({
    date: s.date, n: s.n,
    temp: { min: s.tmin, max: s.tmax },
    dust: { min: s.pmin, max: s.pmax, avg: s.n ? +(s.psum / s.n).toFixed(2) : null },
    smoke: { max: s.smax, detected: s.sover > 0, percent: s.n ? +(100 * s.sover / s.n).toFixed(1) : 0, limit: SMOKE_LIMIT },
    levels: s.levels.map(c => s.n ? +(100 * c / s.n).toFixed(1) : 0),
    hours: s.hours
});
// สรุปวันนี้ (อ่านจากหน่วยความจำ ไม่เปลืองโควตา Firestore)
app.get('/api/summary/today', (req, res) => res.json(view(today)));
// สรุปย้อนหลังหลายวัน ?n=7
app.get('/api/summary/days', wrap(async (req, res) => {
    if (!db) return res.json([]);
    const n = Math.min(parseInt(req.query.n) || 7, 31);
    const snap = await db.collection('daily').orderBy('date', 'desc').limit(n).get();
    res.json(snap.docs.map(d => view(d.data())));
}));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server is running on port ${PORT}`);
});
