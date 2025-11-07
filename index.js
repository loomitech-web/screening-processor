import { Worker } from 'worker_threads';
import { fileURLToPath } from 'url';
import { dirname, join, resolve } from 'path';
import { readFile, writeFile, mkdir } from "fs/promises";
import { MongoClient, ObjectId } from 'mongodb';
import { performance } from 'perf_hooks';
import Redis from 'ioredis';
import SanctionListManager from './SanctionListManager.js'

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const environment = process.env.NODE_ENV || "dev";
const configData = JSON.parse(
    await readFile(new URL("config.json", import.meta.url))
);

const config = configData[environment];
const batchSize = 1000;
const maxThreadCount = 10;
let activeThreadCount = 0;

let manager = null;

let logger = {
    info: console.log,
    error: console.error,
    warn: console.log,
    trace: console.trace
}

const mongoClient = new MongoClient(config.mongo.host, {});
const redisClient = new Redis();
let redisConnected = false;
let scanList = [];
let filterList = [];
let req = {};

let getEnabledInstitutions = async () => {
    try {
        //const enabledAiIds = ["5c0e158a033899600f3257e3"]; //"5c0e158a033899600f3257e3"]; //["5f2296146ce98c190c351da4"]; //, "689b37cf6c8c254be875c896"];
        const institutions = await req.db
            .collection("Institution")
            .find({
                easeficaId: "5c0e158a033899600f3257e3"
            })
            .toArray();

        req.log.info(
            `Institutions with screening enabled: ${institutions
                .map((inst) => inst.easeficaId)
                .join(", ")}`
        );
        return institutions;
    } catch (error) {
        req.log.error("Error getting enabled institutions:", error);
        return [];
    }
}

let getInstitutionScreeningLists = (institution) => {
    const selectedLists = [];

    // Check if institution has the new selectedLists array structure
    if (institution.selectedLists && Array.isArray(institution.selectedLists)) {
        // New structure: selectedLists array with objects
        institution.selectedLists.forEach((list) => {
            if (list.selected && list.code) {
                selectedLists.push(list.code);
            }
        });
    } else {
        // Legacy structure: individual boolean flags
        if (institution.s26AScreening) selectedLists.push("s26A");
        if (institution.s28AScreening) selectedLists.push("s28A");
        if (institution.ofacScreening) selectedLists.push("OFAC");
        if (institution.euSanctionsScreening)
            selectedLists.push("EUFinancialSanctions");
    }

    return selectedLists;
}

let getDataSubjects = async (aiId) => {
    let batches = [];

    const cursor = req.db
        .collection("DataSubject")
        .find({ aiId: aiId, isActive: true })
        .batchSize(batchSize);

    let batch = [];
    let count = 0;

    while (await cursor.hasNext()) {
        const doc = await cursor.next();
        batch.push(doc);
        count++;
        if (batch.length == batchSize) {
            batches.push(batch);
            batch = [];
        }
    }

    if (batch.length) batches.push(batch);

    console.log(`Number of batches for ${aiId} = ${batches.length} TOTAL Data Subject = ${count}`);
    return batches;
}

let results = {

}

let createScreeningResult = async (allResults, filteredResults, data) => {
    let screeningResultRecord;
    for (let result of filteredResults) {
        screeningResultRecord = {
            aiId: data.id,
            timestamp: new Date().getTime(),
            selectedLists: data.lists,
            totalDataSubjects: allResults.length,
            subjectsWithMatches: filteredResults.length,
            totalMatches: result.totalMatches,
        }
    }
    return screeningResultRecord;
}

let createScreeningMatches = async (filteredResults, screeningResultId, screeningResultTimestamp, data) => {
    let matchResultRecords = [];
    for (let result of filteredResults) {
        let matchResultRecord = {
            screeningId: screeningResultId,
            aiId: data.id,
            dataSubject: result.dataSubject,
            matchCount: result.totalMatches,
            lists: data.lists,
            createdAt: screeningResultTimestamp,
            matchDetails: result.matches.map(match => ({
                reference: match.entry.reference,
                source: match.entry.source,
                matchedName: match.matchResult.bestMatchName,
                designation: match.entry.designation,
                country: match.entry.country,
                matchedAttributes:[ {
                    percentageAchieved: match.matchResult.match,
                    bestMatchName: match.matchResult.bestMatchName,
                    details: match.matchResult.details
                }]
            }))
        }
        matchResultRecords.push(matchResultRecord);
    }
    return matchResultRecords;
}

let createMicrotransactionRecord = async(aiId, results, screeningResultTimestamp) => {
    let institution = await req.db.collection("Institution").findOne({ easeficaId: aiId });
    let microtransactionRecord = {
        data: {
            regName: institution.name,
            totalProcessed: results.length,
        }, 
        aiId: aiId,
        type: "ScreeningOnly",
        created: screeningResultTimestamp,
    }
    return microtransactionRecord;
}

let saveScreeningResult = async (screeningResultRecord) => {
    try {
        let screeningResult = await req.db.collection("ScreeningResult").insertOne(screeningResultRecord);
        return {id: screeningResult.insertedId.toHexString(), timestamp: screeningResultRecord.timestamp};
    } catch (error) {
        console.error("Error saving screening result", error);
        return null;
    }
}

let saveMatchResults = async (matchResultRecords) => {
    try {
        let matchResults = await req.db.collection("ScreeningMatch").insertMany(matchResultRecords);
        return matchResults.insertedIds;
    } catch (error) {
        console.error("Error saving match result", error);
        return [];
    }
}

let saveMicrotransaction = async (microtransactionRecord) => {
    try {
        let microtransaction = await req.db.collection("MicroTransactions").insertOne(microtransactionRecord);
        return microtransaction.insertedId.toHexString();
    } catch (error) {
        console.error("Error saving microtransaction", error);
        return null;
    }
}

let startTime = performance.now();

// TODO use this to track old vs new data subject for the day
let setLastScanTime = async (aiId, timestamp) => {
    await req.db.collection("DataSubject").updateMany({ aiId: aiId }, { $set: { lastScanTime: timestamp } });
}

let createWorker = async (data) => {

    activeThreadCount++;

    let worker = new Worker(join(__dirname, 'processAI.js'), {
        workerData: data
    });

    worker.on('message', async(data) => {
        if (data && data.end) {
            activeThreadCount--;
            if (!results[data.id]) {
                results[data.id] = { batchResults: [] }
            }

            results[data.id].batchResults.push(data);

            if (results[data.id].batchResults.length == data.totalBatches) {
                console.log('AI Complete', performance.now() - startTime);
                // TODO accumulate results and save to database

                let allResults = results[data.id].batchResults.flatMap(batch => batch.results);
                let filteredResults = allResults.filter(result => result.totalMatches > 0); 

                // Create & savescreening result record
                let screeningResultRecord = await createScreeningResult(allResults, filteredResults, data);
                let {id: screeningResultId, timestamp: screeningResultTimestamp} = await saveScreeningResult(screeningResultRecord);

                // Create & save match result records
                let matchResultRecords = await createScreeningMatches(filteredResults, screeningResultId, screeningResultTimestamp, data);
                await saveMatchResults(matchResultRecords);

                // Create & save Microtransaction record
                let microtransactionRecord = await createMicrotransactionRecord(data.id, allResults, screeningResultTimestamp);
                await saveMicrotransaction(microtransactionRecord);

                // Update last scan time for all data subjects 
                await setLastScanTime(data.id, screeningResultTimestamp);
            }
        } else {
            console.log(data);
        }
    });

    worker.on('error', console.error);

    return worker;
}

// TODO get this from institution settings
let getThreshold = (institution) => {
    return 75;
}

let wait = async (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
}

let waitForWorkThread = async () => {
    // TODO should loop here until ActiveThreadCount less that MaxThreadCount

    while (activeThreadCount >= maxThreadCount) {
        await wait(500);
        //console.log('activeThreadCount', activeThreadCount);
    }

    return true;
}

let processingLoop = async () => {
    let institutions = await getEnabledInstitutions();

    let startTime = performance.now();
    for (let institution of institutions) {
        let batches = await getDataSubjects(institution.easeficaId);
        let batchNumber = 0;
        for (let batch of batches) {

            await waitForWorkThread();

            let data = {
                aiId: institution.easeficaId,
                batchNumber: batchNumber + 1,
                scanList: scanList,
                dataSubjects: batch,
                totalBatches: batches.length,
                options: {
                    fuzzyThreshold: getThreshold(institution),
                    threshold: getThreshold(institution),
                    sources: getInstitutionScreeningLists(institution)
                }
            }

            await createWorker(data);

            batchNumber++;
        }
    }
}

let init = async () => {

    return new Promise((resolve) => {
        req = {
            log: logger,
            db: mongoClient.db("easefica-screening"),
            redis: redisClient
        }

        manager = new SanctionListManager(req.redis, req.db, req.log);

        manager.monitorProcessedFiles(async (combinedList, type, path) => {
            scanList = combinedList;
            console.log('monitorProcessedFiles');

            // we should get filter list
            filterList = await manager.filterList(filterList);
            await manager.trackEntries(filterList);
        });

        manager.getCombinedSanctionsList().then((combinedList) => {
            scanList = combinedList;
            console.log('getCombinedSanctionsList');
            resolve();
        });
    });
}

let main = async () => {
    redisClient.on("ready", async () => {
        if (redisConnected) return;

        console.log('redis connected.');
        redisConnected = true;

        await mongoClient.connect();
        console.log('db connected.');

        await init();
        processingLoop();
    });
}

main();