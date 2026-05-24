const fs = require("fs");
const path = require("path");
const http = require("http");
const https = require("https");
const express = require("express");
const sanitize = require("sanitize-filename");
const { Server } = require("socket.io");
const { spawn } = require("child_process");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = 3000;
const MAX_CONCURRENT = 10;
const DIRECT_DOWNLOAD_CHUNKS = 8;
const MIN_CHUNK_BYTES = 1024 * 1024;
const MAX_REDIRECTS = 5;

const LINK_FILE = "link.txt";
const COMPLETE_FILE = "complete.txt";
const DOWNLOAD_DIR = "downloads";

if (!fs.existsSync(DOWNLOAD_DIR)) {
    fs.mkdirSync(DOWNLOAD_DIR);
}

if (!fs.existsSync(COMPLETE_FILE)) {
    fs.writeFileSync(COMPLETE_FILE, "");
}

app.use(express.urlencoded({ extended: false }));

let activeDownloads = 0;

const queue = [];
const downloading = new Map();
const downloadControls = new Map();

function escapeHtml(value) {
    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function formatMB(bytes) {
    return (bytes / 1024 / 1024).toFixed(2) + " MB";
}

function formatPercent(downloadedBytes, totalBytes) {
    return totalBytes
        ? ((downloadedBytes / totalBytes) * 100).toFixed(1) + "%"
        : "...";
}

function getProgressValue(progress) {
    const match = String(progress).match(/^(\d+(?:\.\d+)?)%$/);

    if (!match) {
        return 0;
    }

    return Math.min(100, parseFloat(match[1]));
}

function getDownloadId(raw) {
    return Buffer.from(raw).toString("base64url");
}

function getControlById(id) {
    for (const control of downloadControls.values()) {
        if (control.id === id) {
            return control;
        }
    }

    return null;
}

function registerControl(item, type, details = {}) {
    const existing = downloadControls.get(item.raw);
    const control = existing || {
        id: getDownloadId(item.raw),
        item,
        requests: new Set(),
        process: null,
        paused: false,
        canceled: false,
        running: false
    };

    Object.assign(control, {
        type,
        item,
        paused: false,
        canceled: false
    }, details);

    downloadControls.set(item.raw, control);
    return control;
}

function abortControlRequests(control) {
    for (const request of control.requests) {
        request.destroy();
    }

    control.requests.clear();

    if (control.process) {
        control.process.kill("SIGTERM");
    }
}

function removeControlFiles(control) {
    if (control.partPaths) {
        cleanChunkFiles(control.partPaths);
    }

    if (control.output) {
        fs.rmSync(control.output, { force: true });
    }
}

function getHttpClient(url) {
    return url.startsWith("https:") ? https : http;
}

function isHlsUrl(url) {
    try {
        return new URL(url).pathname.toLowerCase().endsWith(".m3u8");
    } catch {
        return false;
    }
}

function getUrlExtension(url) {
    try {
        const extension = path.extname(new URL(url).pathname);
        return extension && extension.length <= 10 ? extension : "";
    } catch {
        return "";
    }
}

function buildDefaultFilename(url) {
    try {
        const parsed = new URL(url);
        const parts = parsed.pathname.split("/").filter(Boolean);
        const lastPart = parts.pop() || "";
        const basePart = lastPart && lastPart !== "index.m3u8"
            ? lastPart
            : parts.pop();
        const name = basePart
            ? path.basename(basePart, path.extname(basePart))
            : "";
        const safeName = sanitize(name);

        if (safeName) {
            return safeName;
        }
    } catch {
        // Fall through to the hash fallback below.
    }

    return "download_" + Buffer.from(url).toString("base64url").slice(0, 12);
}

function ensureOutputFile(item) {
    const extension = isHlsUrl(item.url)
        ? ".mp4"
        : getUrlExtension(item.url);

    const filename = path.extname(item.filename) || !extension
        ? item.filename
        : item.filename + extension;

    return path.join(DOWNLOAD_DIR, filename);
}

function getDownloadedFiles() {
    return fs.readdirSync(DOWNLOAD_DIR, { withFileTypes: true })
        .filter(entry => entry.isFile())
        .filter(entry => !/\.part\d*$/.test(entry.name))
        .map(entry => {
            const filePath = path.join(DOWNLOAD_DIR, entry.name);
            const stat = fs.statSync(filePath);

            return {
                name: entry.name,
                size: formatMB(stat.size),
                updatedAt: stat.mtime
            };
        })
        .sort((a, b) => b.updatedAt - a.updatedAt);
}

let dashboardBroadcastTimer = null;

function getDashboardState() {
    return {
        activeDownloads,
        downloads: Array.from(downloading.values()).map(download => ({
            ...download,
            chunks: download.chunks.map(chunk => ({ ...chunk }))
        })),
        files: getDownloadedFiles()
    };
}

function emitDashboardNow() {
    if (dashboardBroadcastTimer) {
        clearTimeout(dashboardBroadcastTimer);
        dashboardBroadcastTimer = null;
    }

    io.emit("dashboard:update", getDashboardState());
}

function scheduleDashboardBroadcast() {
    if (dashboardBroadcastTimer) return;

    dashboardBroadcastTimer = setTimeout(() => {
        emitDashboardNow();
    }, 150);
}

function setDownloadState(raw, state) {
    downloading.set(raw, state);
    scheduleDashboardBroadcast();
}

function deleteDownloadState(raw) {
    downloading.delete(raw);
    scheduleDashboardBroadcast();
}

function parseLine(line) {
    line = line.trim();

    if (!line) return null;

    const match = line.match(/https?:\/\/\S+/);

    if (!match) return null;

    const url = match[0];

    let filename = line.replace(url, "").trim();

    if (!filename) {
        filename = buildDefaultFilename(url);
    }

    filename = sanitize(filename);

    return {
        raw: line,
        url,
        filename
    };
}

function buildRawLink(url, filename) {
    const cleanUrl = String(url || "").trim();
    const cleanFilename = sanitize(String(filename || "").trim());

    if (!cleanUrl) {
        throw new Error("URL is required");
    }

    try {
        const parsed = new URL(cleanUrl);

        if (!["http:", "https:"].includes(parsed.protocol)) {
            throw new Error("Only http and https links are supported");
        }
    } catch {
        throw new Error("Enter a valid download URL");
    }

    return cleanFilename ? `${cleanUrl} ${cleanFilename}` : cleanUrl;
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

function createDownloadState(item, details = {}) {
    return {
        id: getDownloadId(item.raw),
        filename: item.filename,
        progress: details.progress || "Starting...",
        mb: details.mb || "0 MB",
        speed: details.speed || "0x",
        status: details.status || "Downloading",
        chunks: details.chunks || []
    };
}

function markFailed(item) {
    const current = downloading.get(item.raw) || createDownloadState(item);

    setDownloadState(item.raw, {
        ...current,
        progress: "Failed",
        mb: "-",
        speed: "-",
        status: "Failed"
    });
}

function markPaused(item) {
    const current = downloading.get(item.raw) || createDownloadState(item);

    setDownloadState(item.raw, {
        ...current,
        speed: "-",
        status: "Paused"
    });
}

function markCanceled(item) {
    const current = downloading.get(item.raw) || createDownloadState(item);

    setDownloadState(item.raw, {
        ...current,
        progress: "Canceled",
        speed: "-",
        status: "Canceled"
    });

    setTimeout(() => {
        deleteDownloadState(item.raw);
    }, 3000);
}

function finishDownload(item) {
    const current = downloading.get(item.raw) || createDownloadState(item);

    setDownloadState(item.raw, {
        ...current,
        progress: "100%",
        mb: "Done",
        speed: "-",
        status: "Completed"
    });

    removeFromLinkFile(item.raw);
    moveToComplete(item.raw);
    downloadControls.delete(item.raw);

    setTimeout(() => {
        deleteDownloadState(item.raw);
    }, 5000);
}

function finishDownloadProcess() {
    activeDownloads--;
    processQueue();
    scheduleDashboardBroadcast();
}

function startHlsDownload(item, output, existingControl = null) {
    const control = existingControl || registerControl(item, "hls", { output });

    control.output = output;
    control.running = true;

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
    control.process = ffmpeg;

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

        setDownloadState(item.raw, {
            ...createDownloadState(item),
            progress: timeMatch ? timeMatch[1] : "...",
            mb: sizeMB,
            speed: speedMatch ? speedMatch[1] : "...",
            status: "Downloading"
        });
    });

    ffmpeg.on("close", code => {
        control.running = false;
        control.process = null;

        if (control.paused) {
            fs.rmSync(output, { force: true });
            markPaused(item);
            return;
        }

        if (control.canceled) {
            removeControlFiles(control);
            removeFromLinkFile(item.raw);
            markCanceled(item);
            downloadControls.delete(item.raw);
            finishDownloadProcess();
            return;
        }

        if (code === 0) {
            finishDownload(item);
        } else {
            markFailed(item);
            downloadControls.delete(item.raw);
        }

        finishDownloadProcess();
    });
}

function requestHeaders(url, method = "HEAD", redirectCount = 0) {
    return new Promise((resolve, reject) => {
        const request = getHttpClient(url).request(url, {
            method,
            headers: {
                "Accept-Encoding": "identity"
            }
        }, response => {
            if (
                response.statusCode >= 300 &&
                response.statusCode < 400 &&
                response.headers.location &&
                redirectCount < MAX_REDIRECTS
            ) {
                response.resume();
                const nextUrl = new URL(response.headers.location, url).toString();

                requestHeaders(nextUrl, method, redirectCount + 1)
                    .then(resolve)
                    .catch(reject);
                return;
            }

            response.resume();

            resolve({
                url,
                statusCode: response.statusCode,
                headers: response.headers
            });
        });

        request.on("error", reject);
        request.end();
    });
}

function canUseChunkDownload(headers, size) {
    return (
        size >= MIN_CHUNK_BYTES &&
        String(headers["accept-ranges"] || "").toLowerCase() === "bytes"
    );
}

function buildChunks(totalBytes) {
    const chunkCount = DIRECT_DOWNLOAD_CHUNKS;
    const chunkSize = Math.ceil(totalBytes / chunkCount);
    const chunks = [];

    for (let index = 0; index < chunkCount; index++) {
        const start = index * chunkSize;
        const end = Math.min(start + chunkSize - 1, totalBytes - 1);

        chunks.push({
            id: index + 1,
            start,
            end,
            downloaded: 0,
            total: end - start + 1,
            status: "Waiting"
        });
    }

    return chunks;
}

function updateChunkProgress(item, chunks, startedAt) {
    const downloadedBytes = chunks.reduce(
        (total, chunk) => total + chunk.downloaded,
        0
    );
    const totalBytes = chunks.reduce(
        (total, chunk) => total + chunk.total,
        0
    );
    const elapsedSeconds = Math.max((Date.now() - startedAt) / 1000, 1);

    setDownloadState(item.raw, createDownloadState(item, {
        progress: formatPercent(downloadedBytes, totalBytes),
        mb: `${formatMB(downloadedBytes)} / ${formatMB(totalBytes)}`,
        speed: formatMB(downloadedBytes / elapsedSeconds) + "/s",
        status: "Downloading",
        chunks
    }));
}

function downloadSingleChunk(url, partPath, chunk, item, chunks, startedAt, control) {
    return new Promise((resolve, reject) => {
        if (chunk.downloaded >= chunk.total) {
            chunk.downloaded = chunk.total;
            chunk.status = "Done";
            updateChunkProgress(item, chunks, startedAt);
            resolve();
            return;
        }

        const file = fs.createWriteStream(partPath, { flags: "a" });
        const rangeStart = chunk.start + chunk.downloaded;
        const request = getHttpClient(url).get(url, {
            headers: {
                "Accept-Encoding": "identity",
                Range: `bytes=${rangeStart}-${chunk.end}`
            }
        }, response => {
            if (response.statusCode !== 206) {
                response.resume();
                file.close();
                reject(new Error("Server did not return a byte range"));
                return;
            }

            chunk.status = "Downloading";
            updateChunkProgress(item, chunks, startedAt);

            response.on("data", data => {
                chunk.downloaded += data.length;
                updateChunkProgress(item, chunks, startedAt);
            });

            response.pipe(file);
        });

        control.requests.add(request);

        request.on("error", error => {
            control.requests.delete(request);
            file.close();
            reject(error);
        });

        request.on("close", () => {
            control.requests.delete(request);
        });

        file.on("finish", () => {
            file.close(() => {
                chunk.downloaded = chunk.total;
                chunk.status = "Done";
                updateChunkProgress(item, chunks, startedAt);
                resolve();
            });
        });

        file.on("error", error => {
            file.close();
            reject(error);
        });
    });
}

function mergeChunkFiles(output, partPaths) {
    return new Promise((resolve, reject) => {
        const outputFile = fs.createWriteStream(output);
        let index = 0;

        function appendNext() {
            if (index >= partPaths.length) {
                outputFile.end(resolve);
                return;
            }

            const inputFile = fs.createReadStream(partPaths[index]);

            inputFile.on("error", reject);
            inputFile.on("end", () => {
                index++;
                appendNext();
            });
            inputFile.pipe(outputFile, { end: false });
        }

        outputFile.on("error", reject);
        appendNext();
    });
}

function cleanChunkFiles(partPaths) {
    for (const partPath of partPaths) {
        fs.rmSync(partPath, { force: true });
    }
}

async function startChunkedDownload(
    item,
    url,
    output,
    totalBytes,
    startedAt,
    existingControl = null
) {
    const chunks = existingControl && existingControl.chunks
        ? existingControl.chunks
        : buildChunks(totalBytes);
    const partPaths = existingControl && existingControl.partPaths
        ? existingControl.partPaths
        : chunks.map(chunk => `${output}.part${chunk.id}`);
    const control = existingControl || registerControl(item, "chunked", {
        url,
        output,
        totalBytes,
        chunks,
        partPaths
    });

    Object.assign(control, {
        type: "chunked",
        url,
        output,
        totalBytes,
        chunks,
        partPaths,
        startedAt,
        running: true,
        paused: false,
        canceled: false
    });

    for (const [index, chunk] of chunks.entries()) {
        const partPath = partPaths[index];
        const existingBytes = fs.existsSync(partPath)
            ? fs.statSync(partPath).size
            : 0;

        chunk.downloaded = Math.min(existingBytes, chunk.total);
        chunk.status = chunk.downloaded >= chunk.total ? "Done" : "Waiting";
    }

    setDownloadState(item.raw, createDownloadState(item, {
        progress: formatPercent(
            chunks.reduce((total, chunk) => total + chunk.downloaded, 0),
            totalBytes
        ),
        mb: `${formatMB(chunks.reduce((total, chunk) => total + chunk.downloaded, 0))} / ${formatMB(totalBytes)}`,
        speed: "0 MB/s",
        status: "Downloading",
        chunks
    }));

    try {
        await Promise.all(chunks.map((chunk, index) => {
            return downloadSingleChunk(
                url,
                partPaths[index],
                chunk,
                item,
                chunks,
                startedAt,
                control
            );
        }));

        if (control.paused || control.canceled) {
            throw new Error("Download stopped");
        }

        await mergeChunkFiles(output, partPaths);

        if (control.paused || control.canceled) {
            throw new Error("Download stopped");
        }

        cleanChunkFiles(partPaths);
        finishDownload(item);
        finishDownloadProcess();
    } catch (error) {
        if (control.paused) {
            fs.rmSync(output, { force: true });
            markPaused(item);
            return;
        }

        if (control.canceled) {
            removeControlFiles(control);
            removeFromLinkFile(item.raw);
            markCanceled(item);
            downloadControls.delete(item.raw);
            finishDownloadProcess();
            return;
        }

        abortControlRequests(control);
        cleanChunkFiles(partPaths);
        fs.rmSync(output, { force: true });
        markFailed(item);
        downloadControls.delete(item.raw);
        finishDownloadProcess();
    } finally {
        control.running = false;
    }
}

function startSingleFileDownload(item, url, output, startedAt, existingControl = null) {
    const control = existingControl || registerControl(item, "single", {
        url,
        output
    });
    const partPath = `${output}.part`;
    const file = fs.createWriteStream(partPath);
    let settled = false;

    Object.assign(control, {
        type: "single",
        url,
        output,
        partPaths: [partPath],
        running: true,
        paused: false,
        canceled: false
    });

    function settle(success) {
        if (settled) return;

        settled = true;
        control.running = false;

        if (control.paused) {
            markPaused(item);
            return;
        }

        if (control.canceled) {
            removeControlFiles(control);
            removeFromLinkFile(item.raw);
            markCanceled(item);
            downloadControls.delete(item.raw);
            finishDownloadProcess();
            return;
        }

        if (success) {
            fs.renameSync(partPath, output);
            finishDownload(item);
        } else {
            removeControlFiles(control);
            markFailed(item);
            downloadControls.delete(item.raw);
        }

        finishDownloadProcess();
    }

    const request = getHttpClient(url).get(url, {
        headers: {
            "Accept-Encoding": "identity"
        }
    }, response => {
        if (
            response.statusCode >= 300 &&
            response.statusCode < 400 &&
            response.headers.location
        ) {
            settled = true;
            control.requests.delete(request);
            file.close();
            fs.rmSync(output, { force: true });
            fs.rmSync(partPath, { force: true });

            item.url = new URL(response.headers.location, url).toString();
            startDirectDownload(item, ensureOutputFile(item), control);
            return;
        }

        if (response.statusCode !== 200) {
            response.resume();
            file.close();
            settle(false);
            return;
        }

        const totalBytes = parseInt(response.headers["content-length"] || "0");
        let downloadedBytes = 0;

        response.on("data", chunk => {
            downloadedBytes += chunk.length;

            const elapsedSeconds = Math.max((Date.now() - startedAt) / 1000, 1);
            const speed = formatMB(downloadedBytes / elapsedSeconds) + "/s";
            const percent = totalBytes
                ? ((downloadedBytes / totalBytes) * 100).toFixed(1) + "%"
                : "...";

            setDownloadState(item.raw, {
                ...createDownloadState(item),
                progress: percent,
                mb: totalBytes
                    ? `${formatMB(downloadedBytes)} / ${formatMB(totalBytes)}`
                    : formatMB(downloadedBytes),
                speed,
                status: "Downloading"
            });
        });

        response.pipe(file);
    });

    control.requests.add(request);

    request.on("error", () => {
        control.requests.delete(request);
        file.close();
        settle(false);
    });

    request.on("close", () => {
        control.requests.delete(request);
    });

    file.on("finish", () => {
        file.close(() => {
            settle(true);
        });
    });

    file.on("error", () => {
        file.close();
        settle(false);
    });
}

async function startDirectDownload(item, output, existingControl = null) {
    const control = existingControl || registerControl(item, "pending", { output });
    const startedAt = Date.now();

    Object.assign(control, {
        output,
        running: true
    });

    try {
        const metadata = await requestHeaders(item.url);
        const totalBytes = parseInt(metadata.headers["content-length"] || "0");

        if (control.paused) {
            control.running = false;
            markPaused(item);
            return;
        }

        if (control.canceled) {
            control.running = false;
            removeControlFiles(control);
            removeFromLinkFile(item.raw);
            markCanceled(item);
            downloadControls.delete(item.raw);
            finishDownloadProcess();
            return;
        }

        if (metadata.statusCode >= 400) {
            startSingleFileDownload(item, item.url, output, startedAt, control);
            return;
        }

        item.url = metadata.url;

        if (canUseChunkDownload(metadata.headers, totalBytes)) {
            await startChunkedDownload(
                item,
                metadata.url,
                ensureOutputFile(item),
                totalBytes,
                startedAt,
                control
            );
            return;
        }

        startSingleFileDownload(
            item,
            metadata.url,
            ensureOutputFile(item),
            startedAt,
            control
        );
    } catch (error) {
        if (control.paused) {
            control.running = false;
            markPaused(item);
            return;
        }

        if (control.canceled) {
            control.running = false;
            removeControlFiles(control);
            removeFromLinkFile(item.raw);
            markCanceled(item);
            downloadControls.delete(item.raw);
            finishDownloadProcess();
            return;
        }

        startSingleFileDownload(item, item.url, output, startedAt, control);
    }
}

function startDownload(item) {

    activeDownloads++;

    setDownloadState(item.raw, {
        ...createDownloadState(item)
    });

    const output = ensureOutputFile(item);
    const control = registerControl(item, isHlsUrl(item.url) ? "hls" : "pending", {
        output
    });

    if (isHlsUrl(item.url)) {
        startHlsDownload(item, output, control);
    } else {
        startDirectDownload(item, output, control);
    }
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

app.post("/downloads", (req, res) => {
    try {
        const rawLine = buildRawLink(req.body.url, req.body.filename);

        fs.appendFileSync(LINK_FILE, rawLine + "\n");
        processQueue();

        res.redirect("/");
    } catch (error) {
        res.status(400).send(`
        <html>
        <body>
        <p>${escapeHtml(error.message)}</p>
        <a href="/">Back</a>
        </body>
        </html>
        `);
    }
});

app.post("/downloads/:id/pause", (req, res) => {
    const control = getControlById(req.params.id);

    if (control && !control.paused && !control.canceled) {
        control.paused = true;
        abortControlRequests(control);
        markPaused(control.item);
    }

    res.redirect("/");
});

app.post("/downloads/:id/resume", (req, res) => {
    const control = getControlById(req.params.id);

    if (control && control.paused && !control.canceled && !control.running) {
        control.paused = false;
        control.canceled = false;

        if (control.type === "chunked") {
            startChunkedDownload(
                control.item,
                control.url,
                control.output,
                control.totalBytes,
                Date.now(),
                control
            );
        } else if (control.type === "hls") {
            startHlsDownload(control.item, control.output, control);
        } else if (control.type === "pending") {
            startDirectDownload(control.item, control.output, control);
        } else {
            startSingleFileDownload(
                control.item,
                control.url || control.item.url,
                control.output,
                Date.now(),
                control
            );
        }
    }

    res.redirect("/");
});

app.post("/downloads/:id/cancel", (req, res) => {
    const control = getControlById(req.params.id);

    if (control && !control.canceled) {
        control.canceled = true;
        abortControlRequests(control);

        if (!control.running) {
            removeControlFiles(control);
            removeFromLinkFile(control.item.raw);
            markCanceled(control.item);
            downloadControls.delete(control.item.raw);
            finishDownloadProcess();
        }
    }

    res.redirect("/");
});

app.get("/download/:filename", (req, res) => {
    const filename = path.basename(req.params.filename);
    const filePath = path.join(DOWNLOAD_DIR, filename);

    if (!fs.existsSync(filePath)) {
        res.status(404).send("File not found");
        return;
    }

    res.download(filePath);
});

app.get("/", (req, res) => {
    const html = `
    <html>
    <head>
        <title>Downloader Dashboard</title>

        <style>

        body{
            background:#111;
            color:#fff;
            font-family:Arial;
            padding:20px;
            line-height:1.45;
        }

        .card{
            background:#1e1e1e;
            margin-bottom:10px;
            padding:15px;
            border-radius:8px;
        }

        .row{
            display:flex;
            gap:10px;
            flex-wrap:wrap;
            align-items:end;
        }

        label{
            display:block;
            color:#bbb;
            font-size:13px;
            margin-bottom:6px;
        }

        input{
            background:#0b0b0b;
            border:1px solid #333;
            color:#fff;
            padding:10px;
            min-width:260px;
            border-radius:6px;
        }

        button,
        a.button{
            background:#00b875;
            border:0;
            color:#07130e;
            cursor:pointer;
            display:inline-block;
            font-weight:bold;
            padding:10px 14px;
            border-radius:6px;
            text-decoration:none;
        }

        button.secondary{
            background:#2d7ff9;
            color:#fff;
        }

        button.danger{
            background:#d94141;
            color:#fff;
        }

        .actions{
            display:flex;
            gap:8px;
            flex-wrap:wrap;
            margin-top:14px;
        }

        .actions form{
            margin:0;
        }

        .green{
            color:#00ff99;
        }

        .red{
            color:red;
        }

        .muted{
            color:#aaa;
        }

        .progress-wrap{
            margin-top:14px;
        }

        .progress-info{
            color:#cfcfcf;
            display:flex;
            font-size:13px;
            justify-content:space-between;
            margin-bottom:5px;
        }

        .progress{
            background:#090909;
            border:1px solid #333;
            border-radius:6px;
            height:12px;
            overflow:hidden;
        }

        .progress-bar{
            background:#00b875;
            height:100%;
            min-width:2px;
            transition:width .2s ease;
        }

        .segmented-progress{
            background:#090909;
            border:1px solid #333;
            border-radius:6px;
            display:grid;
            gap:2px;
            height:24px;
            overflow:hidden;
        }

        .progress-segment{
            background:#151515;
            position:relative;
            overflow:hidden;
        }

        .segment-fill{
            background:#00b875;
            height:100%;
            min-width:2px;
            transition:width .2s ease;
        }

        .segment-label{
            color:#fff;
            font-size:11px;
            font-weight:bold;
            left:0;
            line-height:24px;
            position:absolute;
            right:0;
            text-align:center;
            top:0;
        }

        </style>

    </head>

    <body>

    <h1>Live Downloader</h1>

    <div class="card">
        <h2>Add Download</h2>

        <form method="post" action="/downloads" class="row">
            <div>
                <label for="url">URL</label>
                <input id="url" name="url" type="url" required placeholder="https://example.com/file.mp4">
            </div>

            <div>
                <label for="filename">File name</label>
                <input id="filename" name="filename" type="text" placeholder="optional-name">
            </div>

            <button type="submit">Add</button>
        </form>
    </div>

    <h2>
    Active Downloads:
    <span id="active-count">0</span>
    </h2>

    <div id="active-downloads">
        <p class="muted">Connecting...</p>
    </div>

    <h2>Completed Files</h2>

    <div id="completed-files">
        <p class="muted">Connecting...</p>
    </div>

    <script src="/socket.io/socket.io.js"></script>
    <script>
        const socket = io();
        const activeCount = document.getElementById("active-count");
        const activeDownloads = document.getElementById("active-downloads");
        const completedFiles = document.getElementById("completed-files");

        function escapeHtml(value) {
            return String(value)
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }

        function formatPercent(downloadedBytes, totalBytes) {
            return totalBytes
                ? ((downloadedBytes / totalBytes) * 100).toFixed(1) + "%"
                : "...";
        }

        function getProgressValue(progress) {
            const match = String(progress).match(/^(\\d+(?:\\.\\d+)?)%$/);

            if (!match) {
                return 0;
            }

            return Math.min(100, parseFloat(match[1]));
        }

        function renderProgress(download) {
            if (download.chunks && download.chunks.length > 0) {
                return [
                    '<div class="segmented-progress" style="grid-template-columns:repeat(' + download.chunks.length + ',1fr)">',
                    download.chunks.map(chunk => {
                        const percent = chunk.total
                            ? Math.min(100, (chunk.downloaded / chunk.total) * 100)
                            : 0;

                        return [
                            '<div class="progress-segment">',
                            '<div class="segment-fill" style="width:' + percent.toFixed(1) + '%"></div>',
                            '<span class="segment-label">' + escapeHtml(formatPercent(chunk.downloaded, chunk.total)) + '</span>',
                            '</div>'
                        ].join("");
                    }).join(""),
                    '</div>'
                ].join("");
            }

            return [
                '<div class="progress">',
                '<div class="progress-bar" style="width:' + getProgressValue(download.progress).toFixed(1) + '%"></div>',
                '</div>'
            ].join("");
        }

        function renderActions(download) {
            if (
                download.status === "Completed" ||
                download.status === "Canceled" ||
                download.status === "Failed"
            ) {
                return "";
            }

            const controlAction = download.status === "Paused"
                ? '<form method="post" action="/downloads/' + encodeURIComponent(download.id) + '/resume"><button class="secondary" type="submit">Resume</button></form>'
                : '<form method="post" action="/downloads/' + encodeURIComponent(download.id) + '/pause"><button class="secondary" type="submit">Pause</button></form>';

            return [
                '<div class="actions">',
                controlAction,
                '<form method="post" action="/downloads/' + encodeURIComponent(download.id) + '/cancel"><button class="danger" type="submit">Cancel</button></form>',
                '</div>'
            ].join("");
        }

        function renderDownloads(downloads) {
            if (!downloads.length) {
                activeDownloads.innerHTML = '<p class="muted">No active downloads.</p>';
                return;
            }

            activeDownloads.innerHTML = downloads.map(download => {
                const statusClass = download.status === "Failed" ? "red" : "green";
                const chunkText = download.chunks && download.chunks.length > 0
                    ? "Chunks: " + download.chunks.length
                    : "Progress";

                return [
                    '<div class="card">',
                    '<h3>' + escapeHtml(download.filename) + '</h3>',
                    '<p>Status: <span class="' + statusClass + '">' + escapeHtml(download.status) + '</span></p>',
                    '<p>Downloaded: ' + escapeHtml(download.mb) + '</p>',
                    '<p>Progress: ' + escapeHtml(download.progress) + '</p>',
                    '<p>Speed: ' + escapeHtml(download.speed) + '</p>',
                    '<div class="progress-wrap">',
                    '<div class="progress-info">',
                    '<span>' + escapeHtml(chunkText) + '</span>',
                    '<span>' + escapeHtml(download.progress) + '</span>',
                    '</div>',
                    renderProgress(download),
                    '</div>',
                    renderActions(download),
                    '</div>'
                ].join("");
            }).join("");
        }

        function renderFiles(files) {
            if (!files.length) {
                completedFiles.innerHTML = '<p class="muted">No downloaded files yet.</p>';
                return;
            }

            completedFiles.innerHTML = files.map(file => {
                return [
                    '<div class="card">',
                    '<h3>' + escapeHtml(file.name) + '</h3>',
                    '<p>Size: ' + escapeHtml(file.size) + '</p>',
                    '<a class="button" href="/download/' + encodeURIComponent(file.name) + '">Download</a>',
                    '</div>'
                ].join("");
            }).join("");
        }

        socket.on("dashboard:update", state => {
            activeCount.textContent = state.activeDownloads;
            renderDownloads(state.downloads);
            renderFiles(state.files);
        });
    </script>
    </body>
    </html>
    `;

    res.send(html);
});

io.on("connection", socket => {
    socket.emit("dashboard:update", getDashboardState());
});

server.listen(PORT, () => {

    console.log("");
    console.log(`Server Running`);
    console.log(`http://localhost:${PORT}`);
    console.log("");
});

processQueue();
