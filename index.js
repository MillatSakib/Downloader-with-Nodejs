const fs = require("fs");
const path = require("path");
const express = require("express");
const sanitize = require("sanitize-filename");
const { spawn } = require("child_process");

const app = express();

const PORT = 3000;
const MAX_CONCURRENT = 10;

const LINK_FILE = "link.txt";
const COMPLETE_FILE = "complete.txt";
const DOWNLOAD_DIR = "downloads";

if (!fs.existsSync(DOWNLOAD_DIR)) {
    fs.mkdirSync(DOWNLOAD_DIR);
}

if (!fs.existsSync(COMPLETE_FILE)) {
    fs.writeFileSync(COMPLETE_FILE, "");
}

let activeDownloads = 0;

const queue = [];
const downloading = new Map();

function parseLine(line) {
    line = line.trim();

    if (!line) return null;

    const match = line.match(/https?:\/\/\S+/);

    if (!match) return null;

    const url = match[0];

    let filename = line.replace(url, "").trim();

    if (!filename) {
        filename = `video_${Date.now()}`;
    }

    filename = sanitize(filename);

    return {
        raw: line,
        url,
        filename
    };
}

function loadLinks() {
    if (!fs.existsSync(LINK_FILE)) return;

    const raw = fs.readFileSync(LINK_FILE, "utf8");

    const lines = raw
        .split(/\r?\n/)
        .map(v => v.trim())
        .filter(Boolean);

    for (const line of lines) {
        const parsed = parseLine(line);

        if (!parsed) continue;

        const exists =
            queue.find(q => q.raw === parsed.raw) ||
            downloading.has(parsed.raw);

        if (!exists) {
            queue.push(parsed);
        }
    }
}

function removeFromLinkFile(rawLine) {
    const raw = fs.readFileSync(LINK_FILE, "utf8");

    const updated = raw
        .split(/\r?\n/)
        .filter(line => line.trim() !== rawLine.trim())
        .join("\n");

    fs.writeFileSync(LINK_FILE, updated);
}

function moveToComplete(rawLine) {
    fs.appendFileSync(COMPLETE_FILE, rawLine + "\n");
}

function startDownload(item) {

    activeDownloads++;

    downloading.set(item.raw, {
        filename: item.filename,
        progress: "Starting...",
        mb: "0 MB",
        speed: "0x",
        status: "Downloading"
    });

    const output = path.join(
        DOWNLOAD_DIR,
        `${item.filename}.mp4`
    );

    const args = [
        "-i",
        item.url,
        "-c",
        "copy",
        "-bsf:a",
        "aac_adtstoasc",
        output,
        "-y"
    ];

    const ffmpeg = spawn("ffmpeg", args);

    ffmpeg.stderr.on("data", data => {

        const text = data.toString();

        const timeMatch = text.match(/time=(\S+)/);
        const speedMatch = text.match(/speed=\s*([^\s]+)/);
        const sizeMatch = text.match(/size=\s*(\d+)kB/);

        let sizeMB = "0 MB";

        if (sizeMatch) {
            sizeMB =
                (parseInt(sizeMatch[1]) / 1024).toFixed(2) + " MB";
        }

        downloading.set(item.raw, {
            filename: item.filename,
            progress: timeMatch ? timeMatch[1] : "...",
            mb: sizeMB,
            speed: speedMatch ? speedMatch[1] : "...",
            status: "Downloading"
        });
    });

    ffmpeg.on("close", code => {

        if (code === 0) {

            downloading.set(item.raw, {
                filename: item.filename,
                progress: "100%",
                mb: "Done",
                speed: "-",
                status: "Completed"
            });

            removeFromLinkFile(item.raw);
            moveToComplete(item.raw);

            setTimeout(() => {
                downloading.delete(item.raw);
            }, 5000);

        } else {

            downloading.set(item.raw, {
                filename: item.filename,
                progress: "Failed",
                mb: "-",
                speed: "-",
                status: "Failed"
            });
        }

        activeDownloads--;

        processQueue();
    });
}

function processQueue() {

    loadLinks();

    while (
        activeDownloads < MAX_CONCURRENT &&
        queue.length > 0
    ) {

        const item = queue.shift();

        if (!item) break;

        startDownload(item);
    }
}

setInterval(() => {
    processQueue();
}, 3000);

app.get("/", (req, res) => {

    let html = `
    <html>
    <head>
        <title>Downloader Dashboard</title>

        <meta http-equiv="refresh" content="2">

        <style>

        body{
            background:#111;
            color:#fff;
            font-family:Arial;
            padding:20px;
        }

        .card{
            background:#1e1e1e;
            margin-bottom:10px;
            padding:15px;
            border-radius:10px;
        }

        .green{
            color:#00ff99;
        }

        .red{
            color:red;
        }

        </style>

    </head>

    <body>

    <h1>Live Downloader</h1>

    <h2>
    Active Downloads:
    ${activeDownloads}
    </h2>
    `;

    downloading.forEach(v => {

        html += `
        <div class="card">

        <h3>${v.filename}</h3>

        <p>Status: ${
            v.status === "Failed"
                ? `<span class="red">${v.status}</span>`
                : `<span class="green">${v.status}</span>`
        }</p>

        <p>Downloaded: ${v.mb}</p>

        <p>Progress Time: ${v.progress}</p>

        <p>Speed: ${v.speed}</p>

        </div>
        `;
    });

    html += `
    </body>
    </html>
    `;

    res.send(html);
});

app.listen(PORT, () => {

    console.log("");
    console.log(`Server Running`);
    console.log(`http://localhost:${PORT}`);
    console.log("");
});

processQueue();