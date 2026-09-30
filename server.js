const express = require('express');
const http = require('http');
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static('public')); // ตั้งค่าโฟลเดอร์สำหรับหน้าเว็บ index.html

// รอรับค่า POST จาก ESP32
app.post('/update', (req, res) => {
    const data = req.body;
    console.log("Received from ESP32:", data);
    io.emit('sensor_update', data); // ยิงข้อมูลส่งไปที่หน้าต่างเบราว์เซอร์
    res.sendStatus(200);
});

// ใช้ process.env.PORT เพื่อให้ระบบ Render สุ่ม Port ให้ทำงานได้โดยไม่ชนกัน
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server is running on port ${PORT}`);
});