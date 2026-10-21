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
// FIREBASE_DB_URL          : URL ของ Realtime Database (ไม่ใส่ = ใช้ค่าเริ่มต้นด้านล่าง)
const API_KEY = process.env.API_KEY;
const FIELDS = ['temperature', 'humidity', 'pm25', 'smoke'];

let db = null;
try {
    if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        admin.initializeApp({
            credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)),
            databaseURL: process.env.FIREBASE_DB_URL || 'https://esp32-dht11-monitor-d5e4f-default-rtdb.firebaseio.com'
        });
        db = admin.database();
        console.log('Realtime Database connected');
    } else {
        console.warn('FIREBASE_SERVICE_ACCOUNT not set: history is disabled');
    }
} catch (e) {
    console.error('Firebase init failed:', e.message);
}

// Realtime Database ไม่รับค่า undefined และไม่เก็บ null -> ล้างก่อนบันทึกทุกครั้ง
const clean = o => JSON.parse(JSON.stringify(o));

let latest = null;  // ค่าล่าสุดที่ ESP32 ส่งมา
let lastSeen = 0;   // เวลา (ms) ที่ได้รับข้อมูลล่าสุด
let bucket = null;  // ตัวสะสมค่าเฉลี่ยของนาทีปัจจุบัน

// บันทึกค่าเฉลี่ยของ 1 นาทีลง readings/{timestamp}
async function flush(b) {
    if (!db || !b) return;
    const doc = { ts: b.key, n: b.n };
    FIELDS.forEach(k => { doc[k] = +(b.sum[k] / b.n).toFixed(2); });
    try {
        await db.ref('readings/' + b.key).set(doc);
    } catch (e) {
        console.error('Database write failed:', e.message);
    }
}

// ===== สรุปรายวัน (เวลาไทย) เก็บที่ daily/{YYYY-MM-DD} =====
const SMOKE_LIMIT = 80;                        // ให้ตรงกับเฟิร์มแวร์
const DUST_CUT = [15, 25, 37.5, 75];           // เกณฑ์ฝุ่น 5 ระดับ (ตรงกับเฟิร์มแวร์)
const thai = ts => new Date(ts + 7 * 3600e3);  // เลื่อนเป็นเวลาไทย แล้วอ่านด้วย getUTC*
const dayKey = ts => thai(ts).toISOString().slice(0, 10);
const emptyDay = key => ({ date: key, n: 0, tmin: null, tmax: null, pmin: null, pmax: null,
    psum: 0, smax: 0, sover: 0, levels: [0, 0, 0, 0, 0], hours: {} });
let today = emptyDay(dayKey(Date.now()));

async function saveDaily() {
    if (!db) return;
    try { await db.ref('daily/' + today.date).set(clean(today)); }
    catch (e) { console.error('Daily write failed:', e.message); }
}

// Realtime Database อาจคืน hours เป็น array (ถ้าคีย์เป็นเลขเรียงกัน) -> แปลงกลับเป็น object
function fromDb(v, key) {
    const d = Object.assign(emptyDay(key), v);
    const hours = {};
    Object.keys(d.hours || {}).forEach(h => { if (d.hours[h]) hours[h] = d.hours[h]; });
    d.hours = hours;
    d.levels = [0, 1, 2, 3, 4].map(i => (d.levels && d.levels[i]) || 0);
    return d;
}

// เซิร์ฟเวอร์รีสตาร์ทกลางวัน -> โหลดสรุปของวันนี้กลับมาต่อ
if (db) db.ref('daily/' + today.date).once('value')
    .then(s => { if (s.exists() && today.n === 0) today = fromDb(s.val(), today.date); })
    .catch(e => console.error('Daily load failed:', e.message));

function addDaily(d, ts) {
    const key = dayKey(ts);
    if (key !== today.date) { saveDaily(); today = emptyDay(key); } // ขึ้นวันใหม่
    const lo = (a, v) => a == null ? v : Math.min(a, v);
    const hi = (a, v) => a == null ? v : Math.max(a, v);
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
        saveDaily();   // บันทึกสรุปรายวัน (1 ครั้ง/นาที)
        bucket = { key, n: 0, sum: { temperature: 0, humidity: 0, pm25: 0, smoke: 0 } };
    }
    bucket.n++;
    FIELDS.forEach(k => { bucket.sum[k] += d[k]; });

    res.sendStatus(200);
});

// ===== REST API =====
const RANGES = { '1h': 3600e3, '6h': 21600e3, '24h': 86400e3 };
const cache = {}; // แคช 30 วินาที ลดจำนวนการอ่านฐานข้อมูล

async function getHistory(range) {
    const r = RANGES[range] ? range : '1h';
    if (!db) return [];
    if (cache[r] && Date.now() - cache[r].at < 30000) return cache[r].rows;
    // คีย์ของ readings คือ timestamp (ms) ความยาวเท่ากัน จึงเรียงและกรองด้วยคีย์ได้
    const snap = await db.ref('readings').orderByKey()
        .startAt(String(Date.now() - RANGES[r])).limitToLast(1500).once('value');
    const rows = Object.values(snap.val() || {}).sort((a, b) => a.ts - b.ts);
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
// สรุปวันนี้ (อ่านจากหน่วยความจำ ไม่เปลืองโควตา)
app.get('/api/summary/today', (req, res) => res.json(view(today)));
// สรุปย้อนหลังหลายวัน ?n=7
app.get('/api/summary/days', wrap(async (req, res) => {
    if (!db) return res.json([]);
    const n = Math.min(parseInt(req.query.n) || 7, 31);
    const snap = await db.ref('daily').orderByKey().limitToLast(n).once('value');
    const days = Object.entries(snap.val() || {}).map(([k, v]) => fromDb(v, k));
    res.json(days.reverse().map(view)); // วันล่าสุดอยู่บนสุด
}));

// ===== API คำนวณ อุณหภูมิ / ฝุ่น =====
const LEVEL_NAMES = ['ดีมาก', 'ดี', 'ปานกลาง', 'สะสมมาก', 'วิกฤต']; // ตรงกับ DUST_CUT และหน้าสรุปรายวัน

// PM2.5 -> US AQI (มาตรฐาน US EPA)
const AQI_TABLE = [
    // [pmต่ำ, pmสูง, aqiต่ำ, aqiสูง, ระดับ AQI]
    [0,     12.0,  0,   50,  'Good'],
    [12.1,  35.4,  51,  100, 'Moderate'],
    [35.5,  55.4,  101, 150, 'Unhealthy for Sensitive Groups'],
    [55.5,  150.4, 151, 200, 'Unhealthy'],
    [150.5, 250.4, 201, 300, 'Very Unhealthy'],
    [250.5, 500.4, 301, 500, 'Hazardous']
];
function calcDust(pm) {
    const c = Math.floor(pm * 10) / 10; // ตัดเหลือ 1 ตำแหน่งตามมาตรฐาน
    const r = AQI_TABLE.find(x => c >= x[0] && c <= x[1]) || AQI_TABLE[AQI_TABLE.length - 1];
    const aqi = Math.min(500, Math.round((r[3] - r[2]) / (r[1] - r[0]) * (c - r[0]) + r[2]));
    return {
        pm25: +pm.toFixed(2),
        aqi,
        aqiLevel: r[4],
        level: LEVEL_NAMES[DUST_CUT.filter(x => pm > x).length] // ระดับ 5 ขั้นเดียวกับเฟิร์มแวร์
    };
}

// อุณหภูมิ: แปลงหน่วย + Heat Index (อุณหภูมิที่รู้สึกจริง) + จุดน้ำค้าง + สถานะ
function calcTemp(c, rh) {
    const f = c * 9 / 5 + 32;
    let hiF = f; // สูตร Rothfusz ใช้ได้เมื่อ >= 80°F
    if (f >= 80) {
        hiF = -42.379 + 2.04901523 * f + 10.14333127 * rh - 0.22475541 * f * rh
            - 0.00683783 * f * f - 0.05481717 * rh * rh + 0.00122874 * f * f * rh
            + 0.00085282 * f * rh * rh - 0.00000199 * f * f * rh * rh;
    }
    // จุดน้ำค้าง (สูตร Magnus)
    const a = 17.62, b = 243.12;
    const g = Math.log(Math.max(rh, 0.1) / 100) + a * c / (b + c);
    const dew = b * g / (a - g);
    const status = c >= 45 ? 'ร้อนอันตราย' : c >= 35 ? 'ร้อน' : c >= 20 ? 'ปกติ' : 'เย็น';
    return {
        celsius: +c.toFixed(1),
        fahrenheit: +f.toFixed(1),
        kelvin: +(c + 273.15).toFixed(2),
        humidity: +rh.toFixed(1),
        feelsLikeC: +((hiF - 32) * 5 / 9).toFixed(1),
        dewPointC: +dew.toFixed(1),
        status
    };
}

// GET /api/calc                          -> คำนวณจากค่าล่าสุดของเซนเซอร์
// GET /api/calc?temp=32&hum=45&pm25=20   -> คำนวณจากค่าที่ส่งมาเอง
app.get('/api/calc', (req, res) => {
    const num = (q, fb) => (q !== undefined && q !== '' && isFinite(+q)) ? +q : fb;
    const temp = num(req.query.temp, latest && latest.temperature);
    const hum  = num(req.query.hum,  latest && latest.humidity);
    const pm   = num(req.query.pm25, latest && latest.pm25);
    if (temp == null || pm == null) return res.status(404).json({ error: 'ยังไม่มีข้อมูล' });
    res.json({
        temperature: calcTemp(temp, hum == null ? 50 : hum),
        dust: calcDust(pm)
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server is running on port ${PORT}`);
});
