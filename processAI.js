import { Worker, parentPort, workerData } from 'worker_threads';
import { fileURLToPath } from 'url';
import { dirname, join, resolve } from 'path';
import { readFile, writeFile, mkdir } from "fs/promises";
import { MongoClient, ObjectId } from 'mongodb';
import Redis from 'ioredis';
import SanctionsScanner from './SanctionsScanner.js'

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const environment = process.env.NODE_ENV || "dev";
const configData = JSON.parse(
    await readFile(new URL("config.json", import.meta.url))
);
const config = configData[environment];

let logger = {
    info: console.log,
    error: console.error,
    warn: console.log,
    trace: console.trace
}

const mongoClient = new MongoClient(config.mongo.host, {});
const redisClient = new Redis();
let redisConnected = false;
let results = [];
let main = async () => {

    redisClient.on("ready", async () => {
        if (redisConnected) return;

        redisConnected = true;

        await mongoClient.connect();
        parentPort.postMessage({ id: workerData.aiId, batchNumber: workerData.batchNumber, totalDataSubjects: workerData.dataSubjects.length, message: "about to start processing" });

        let req = {
            log: logger
        }

        let scanner = new SanctionsScanner(req);

        scanner.setSanctionsList(workerData.scanList);

        let options = workerData.options;

        let count = 0;
        for (let dataSubject of workerData.dataSubjects) {
            let searchTerm = [
                dataSubject.dataSubject.firstName,
                dataSubject.dataSubject.secondName,
                dataSubject.dataSubject.thirdName,
                dataSubject.dataSubject.fourthName,
                dataSubject.dataSubject.lastName,
            ]
                .filter((part) => part && part.trim())
                .join(" ");

            searchTerm.trim();

            options.entityType = dataSubject.entityType == "Natural Person" ? "Individual" : "Institution";

            let scanResult = await scanner.internalMatch(searchTerm, options);

            // TODO accumulate results
            //console.log("Scan result", scanResult);
            results.push({...scanResult, dataSubject: dataSubject.dataSubject});

            count++;
            if (count % 100 == 0) { // show process in log every 100 records
                parentPort.postMessage({ id: workerData.aiId, batchNumber: workerData.batchNumber, message: count + ' data subjects scanned' });
            }
        }

        mongoClient.close();
        parentPort.postMessage({ id: workerData.aiId, message: 'db closed.' });
        // console.log("Results that are being sent to the parent", results);
        await redisClient.quit();
        parentPort.postMessage({ id: workerData.aiId, batchNumber: workerData.batchNumber, totalBatches: workerData.totalBatches, message: 'thread ending.', end: true, results: results, lists: workerData.options.sources });
    });
}

main();