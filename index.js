const express = require("express");
const noble   = require("@abandonware/noble");

const app  = express();
const PORT = 4200;

const FLOWER_CARE = {
    serviceUUID:     "1204",
    dataCharUUID:    "1a01",
    firmwareCharUUID:"1a02",
    modeCharUUID:    "1a00",
    realTimeModeCmd: Buffer.from([0xa0, 0x1f]),
};

const MI_TEMP = {
    serviceUUID: "ebe0ccb0-7a0a-4b0c-8a1a-6ff2997da3a6",
    dataCharUUID:"ebe0ccc1-7a0a-4b0c-8a1a-6ff2997da3a6",
};

const SCAN_TIMEOUT_MS      = 60000;
const CONNECT_TIMEOUT_MS   = 10000;
const DISCOVERY_TIMEOUT_MS = 10000;
const RETRY_DELAY_MS       = 2000;
const MAX_ATTEMPTS         = 4;

// ─────────────────────────────────────────────
// Вспомогательные функции
// ─────────────────────────────────────────────

function waitForBluetooth() {
    return new Promise((resolve, reject) => {
        if (noble.state === "poweredOn") return resolve();
        const timer = setTimeout(
            () => reject(new Error("Таймаут ожидания Bluetooth")), 5000
        );
        noble.once("stateChange", (state) => {
            clearTimeout(timer);
            if (state === "poweredOn") resolve();
            else reject(new Error(`Bluetooth не доступен: ${state}`));
        });
    });
}

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

/**
 * Сканирует BLE-устройства и возвращает найденные peripheral по ключам.
 * Останавливает сканирование когда все targets найдены или вышел таймаут.
 */
function scanForTargets(targets, timeoutMs) {
    return new Promise((resolve, reject) => {
        const found = {};
        let scanning = true;

        const tryStop = () => {
            if (!scanning) return;
            if (targets.every((t) => found[t.key])) {
                scanning = false;
                noble.stopScanning();
                clearTimeout(timer);
                noble.removeListener("discover", onDiscover);
                resolve(found);
            }
        };

        const onDiscover = (peripheral) => {
            const localName = peripheral.advertisement.localName || "";
            for (const target of targets) {
                if (!found[target.key] && target.names.some((n) => localName.includes(n))) {
                    console.log(`[SCAN] Найдено: ${localName} (${peripheral.address})`);
                    found[target.key] = peripheral;
                    tryStop();
                }
            }
        };

        noble.on("discover", onDiscover);

        const timer = setTimeout(() => {
            if (!scanning) return;
            scanning = false;
            noble.stopScanning();
            noble.removeListener("discover", onDiscover);
            if (Object.keys(found).length === 0) {
                reject(new Error("Устройства не найдены за отведённое время"));
            } else {
                resolve(found);
            }
        }, timeoutMs);

        noble.startScanning([], true);
    });
}

function connectWithTimeout(peripheral, timeoutMs) {
    return new Promise((resolve, reject) => {
        if (peripheral.state === "connected") return resolve();

        const timer = setTimeout(
            () => reject(new Error(`Таймаут подключения к ${peripheral.address}`)),
            timeoutMs
        );

        const onDisconnect = () => {
            clearTimeout(timer);
            reject(new Error(`${peripheral.address} отключился во время connect`));
        };
        peripheral.once("disconnect", onDisconnect);

        peripheral.connect((err) => {
            clearTimeout(timer);
            peripheral.removeListener("disconnect", onDisconnect);
            if (err) reject(err);
            else resolve();
        });
    });
}

async function disconnect(peripheral) {
    return new Promise((resolve) => {
        if (peripheral.state === "disconnected") return resolve();
        peripheral.disconnect(() => resolve());
    });
}

async function forceDisconnect(peripheral) {
    await disconnect(peripheral);
    await sleep(600);
}

function discoverCharacteristics(peripheral, serviceUUID, charUUIDs) {
    return new Promise((resolve, reject) => {
        const cleanService = serviceUUID.replace(/-/g, "");
        const cleanChars   = charUUIDs.map((u) => u.replace(/-/g, ""));

        let settled = false;
        const finish = (err, result) => {
            if (settled) return;
            settled = true;
            peripheral.removeListener("disconnect", onDisconnect);
            clearTimeout(timer);
            err ? reject(err) : resolve(result);
        };

        const timer = setTimeout(
            () => finish(new Error(`Discovery завис (${DISCOVERY_TIMEOUT_MS}ms)`)),
            DISCOVERY_TIMEOUT_MS
        );

        const onDisconnect = () =>
            finish(new Error(`${peripheral.address} отключился во время discovery`));
        peripheral.once("disconnect", onDisconnect);

        peripheral.discoverSomeServicesAndCharacteristics(
            [cleanService],
            cleanChars,
            (err, _services, characteristics) => {
                if (err) return finish(err);
                if (!characteristics || characteristics.length === 0)
                    return finish(new Error(`Характеристики не найдены: ${charUUIDs.join(", ")}`));
                const map = {};
                for (const c of characteristics) map[c.uuid] = c;
                finish(null, map);
            }
        );
    });
}

function readCharacteristic(char) {
    return new Promise((resolve, reject) => {
        char.read((err, data) => (err ? reject(err) : resolve(data)));
    });
}

function writeCharacteristic(char, buffer, withoutResponse = false) {
    return new Promise((resolve, reject) => {
        char.write(buffer, withoutResponse, (err) => (err ? reject(err) : resolve()));
    });
}

async function withRetry(label, peripheral, fn) {
    let lastErr;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        try {
            return await fn();
        } catch (e) {
            lastErr = e;
            console.warn(`[${label}] Попытка ${attempt}/${MAX_ATTEMPTS}: ${e.message}`);
            if (attempt < MAX_ATTEMPTS) {
                await forceDisconnect(peripheral);
                await sleep(RETRY_DELAY_MS);
            }
        }
    }
    throw lastErr;
}

// ─────────────────────────────────────────────
// Опрос Flower Care
// ─────────────────────────────────────────────
async function readFlowerCare(peripheral) {
    return withRetry("FlowerCare", peripheral, async () => {
        console.log("[FlowerCare] Подключение...");
        await connectWithTimeout(peripheral, CONNECT_TIMEOUT_MS);
        console.log("[FlowerCare] Подключено");

        try {
            const chars = await discoverCharacteristics(
                peripheral,
                FLOWER_CARE.serviceUUID,
                [FLOWER_CARE.modeCharUUID, FLOWER_CARE.dataCharUUID, FLOWER_CARE.firmwareCharUUID]
            );

            const modeChar = chars[FLOWER_CARE.modeCharUUID];
            const dataChar = chars[FLOWER_CARE.dataCharUUID];
            const fwChar   = chars[FLOWER_CARE.firmwareCharUUID];

            if (!modeChar || !dataChar || !fwChar)
                throw new Error("Не все характеристики найдены");

            await writeCharacteristic(modeChar, FLOWER_CARE.realTimeModeCmd, false);

            const [, fwRaw] = await Promise.all([
                sleep(600),
                readCharacteristic(fwChar),
            ]);

            const raw = await readCharacteristic(dataChar);

            return {
                temperature: raw.readUInt16LE(0) / 10,
                lux:         raw.readUInt32LE(3),
                moisture:    raw.readUInt8(7),
                fertility:   raw.readUInt16LE(8),
                battery:     fwRaw.readUInt8(0),
                firmware:    fwRaw.slice(2).toString("ascii").replace(/\0/g, ""),
            };
        } finally {
            await disconnect(peripheral);
            console.log("[FlowerCare] Отключено");
        }
    });
}

// ─────────────────────────────────────────────
// Опрос Mi Temp & Humidity Monitor
// ─────────────────────────────────────────────
async function readMiTemp(peripheral) {
    return withRetry("MiTemp", peripheral, async () => {
        console.log("[MiTemp] Подключение...");
        await connectWithTimeout(peripheral, CONNECT_TIMEOUT_MS);
        console.log("[MiTemp] Подключено");

        try {
            const chars = await discoverCharacteristics(
                peripheral,
                MI_TEMP.serviceUUID,
                [MI_TEMP.dataCharUUID]
            );

            const key      = MI_TEMP.dataCharUUID.replace(/-/g, "");
            const dataChar = chars[key];
            if (!dataChar) throw new Error("dataChar не найден");

            const raw     = await readCharacteristic(dataChar);
            const voltage = raw.readUInt16LE(3) / 1000;

            return {
                temperature: raw.readInt16LE(0) / 100,
                humidity:    raw.readUInt8(2),
                voltage,
                battery: Math.min(100, Math.max(0, Math.round((voltage - 2.1) * 100))),
            };
        } finally {
            await disconnect(peripheral);
            console.log("[MiTemp] Отключено");
        }
    });
}

// ─────────────────────────────────────────────
// GET /
// ─────────────────────────────────────────────
app.get("/", async (req, res) => {
    const result = { flowerCare: null, miTemp: null, errors: [] };

    try {
        await waitForBluetooth();
        console.log("[SCAN] Начинаем сканирование...");

        const targets = [
            { key: "flowerCare", names: ["Flower care", "Flower Care", "MiFlora"] },
            { key: "miTemp",     names: ["LYWSD03MMC", "MJ_HT_V1", "ClearGrass"] },
        ];

        // Сначала сканируем — ждём пока найдём оба устройства (или выйдет таймаут)
        const found = await scanForTargets(targets, SCAN_TIMEOUT_MS);
        console.log("[SCAN] Сканирование завершено, начинаем опрос устройств...");

        // Последовательный опрос: сначала FlowerCare, потом MiTemp
        if (found.flowerCare) {
            console.log("[FLOW] Опрашиваем FlowerCare...");
            try {
                result.flowerCare = await readFlowerCare(found.flowerCare);
                console.log("[FLOW] FlowerCare успешно опрошен");
            } catch (e) {
                result.errors.push(`FlowerCare: ${e.message}`);
                console.error("[FLOW] Ошибка FlowerCare:", e.message);
            }
        } else {
            result.errors.push("FlowerCare: устройство не найдено");
        }

        if (found.miTemp) {
            console.log("[FLOW] Опрашиваем MiTemp...");
            try {
                result.miTemp = await readMiTemp(found.miTemp);
                console.log("[FLOW] MiTemp успешно опрошен");
            } catch (e) {
                result.errors.push(`MiTemp: ${e.message}`);
                console.error("[FLOW] Ошибка MiTemp:", e.message);
            }
        } else {
            result.errors.push("MiTemp: устройство не найдено");
        }

        // Оба устройства опрошены — возвращаем результат
        res.json({ success: true, data: result });

    } catch (err) {
        console.error("[ERROR]", err.message);
        res.status(500).json({ success: false, error: err.message });
    }
});

// ─────────────────────────────────────────────
app.listen(PORT, () => {
    console.log(`Сервер запущен: http://localhost:${PORT}`);
    console.log(`Запрос данных: GET http://localhost:${PORT}`);
});