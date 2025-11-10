import { Worker } from 'worker_threads';
import { fileURLToPath } from 'url';
import { dirname, join, resolve } from 'path';
import { readFile, writeFile, mkdir } from "fs/promises";
import { MongoClient, ObjectId } from 'mongodb';
import { performance } from 'perf_hooks';
import Redis from 'ioredis';
import SanctionListManager from './SanctionListManager.js'
import cron from 'node-cron';

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
let institutionCompletedCount = 0;
let institutionCount = 0;
let institutions = [];

let getEnabledInstitutions = async () => {
    try {
        const enabledAiIds = ["63ff6046aea3ec7785c4ecdc"]; // ["5c0e158a033899600f3257e3"]; //"5c0e158a033899600f3257e3"]; //["5f2296146ce98c190c351da4"]; //, "689b37cf6c8c254be875c896"];
        const institutions = await req.db
            .collection("Institution")
            .find({
                isScreening: true,
                easeficaId: { $ne: "63ff6046aea3ec7785c4ecdc" }
                //easeficaId: { $in: enabledAiIds }
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
    let screeningResultRecord = {
        aiId: data.id,
        timestamp: new Date().getTime(),
        selectedLists: data.lists,
        totalDataSubjects: allResults.length,
        subjectsWithMatches: filteredResults.length,
        totalMatches: 0,
    };

    // If there are filtered results, update totalMatches with the last result's value
    if (filteredResults.length > 0) {
        screeningResultRecord.totalMatches = filteredResults[filteredResults.length - 1].totalMatches;
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
                matchedAttributes: [{
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

let createMicrotransactionRecord = async (aiId, results, screeningResultTimestamp) => {
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
        return { id: screeningResult.insertedId.toHexString(), timestamp: screeningResultRecord.timestamp };
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
    await req.db.collection("DataSubject").updateMany({ aiId: aiId, isActive: true }, { $set: { lastScanTime: timestamp } });
}

let createWorker = async (data) => {

    activeThreadCount++;

    let worker = new Worker(join(__dirname, 'processAI.js'), {
        workerData: data
    });

    worker.on('message', async (data) => {
        if (data && data.end) {
            activeThreadCount--;
            if (!results[data.id]) {
                results[data.id] = { batchResults: [] }
            }

            results[data.id].batchResults.push(data);

            if (results[data.id].batchResults.length == data.totalBatches) {
                console.log(`AI ${data.id} Complete`, performance.now() - data.startTime);
                let allResults = results[data.id].batchResults.flatMap(batch => batch.results);
                let filteredResults = allResults.filter(result => result.totalMatches > 0);

                // Create & savescreening result record
                let screeningResultRecord = await createScreeningResult(allResults, filteredResults, data);
                let { id: screeningResultId, timestamp: screeningResultTimestamp } = await saveScreeningResult(screeningResultRecord);

                // Create & save match result records if there are any matches
                if (filteredResults.length > 0) {
                    let matchResultRecords = await createScreeningMatches(filteredResults, screeningResultId, screeningResultTimestamp, data);
                    await saveMatchResults(matchResultRecords);
                }

                // Create & save Microtransaction record
                let microtransactionRecord = await createMicrotransactionRecord(data.id, allResults, screeningResultTimestamp);
                await saveMicrotransaction(microtransactionRecord);

                institutionCompletedCount++;

                console.log('Institution Completed', institutionCompletedCount);

                // Update last scan time for all data subjects 
                await setLastScanTime(data.id, screeningResultTimestamp);

                if (institutionCompletedCount == institutionCount) {
                    console.log('All institutions processed. Cleaning up and exiting...');

                    // Close connections if needed
                    await mongoClient.close();
                    redisClient.disconnect();

                    process.exit(0);
                }
            } else {
                console.log(data.id, data.batchNumber, data.message);
            }
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
    req.log.info('Downloading Sanctions Lists');
    await manager.downloadLists(); // fetch XML files form source and save to processed lists
    req.log.info('Getting Combined Sanctions List');
    scanList = await manager.getCombinedSanctionsList(); // load all entries from processed lists
    req.log.info('Filtering New Entries');
    let filter = await manager.filterNewEntries(scanList); // compares all entries md5 against stored md5 hases in redis
    req.log.info('New Entries', filter.newEntries.length);
    req.log.info('Clearing Sanction List Entries');
    await manager.clearSanctionListEntries(); // clear existing md5 hashes used for compare
    req.log.info('Tracking Entries');
    await manager.trackEntries(scanList); // creates md5 hashes in redis for compare in the next cycle
    req.log.info('Combined Sanctions List', scanList.length);

    institutions = await getEnabledInstitutions();
    institutionCompletedCount = 0;


    let startTime = performance.now();
    for (let institution of institutions) {
        let batches = await getDataSubjects(institution.easeficaId);

        if (batches.length > 0) {
            institutionCount++;
            console.log('Institution', institutionCount, institution.easeficaId);

            let startTime = performance.now();
            let batchNumber = 0;
            for (let batch of batches) {

                await waitForWorkThread();

                let data = {
                    aiId: institution.easeficaId,
                    batchNumber: batchNumber + 1,
                    startTime: startTime,
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
        await processingLoop();
        // Schedule processingLoop to run at midnight every day
        /*const cronExpression = `0 0 * * *`;
        const task = cron.schedule(cronExpression, async () => {
            console.log("Starting Daily Data Subject Screening at ", new Date().toISOString());
            try {
                await processingLoop();
                console.log("Daily Data Subject Screening Completed");
            } catch (error) {
                console.error("Error during scheduled screening:", error);
            }
        }, {
            scheduled: true,
            timezone: Intl.DateTimeFormat().resolvedOptions().timeZone // Use system timezone
        });

        // Verify the task was created
        if (task) {
            console.log("Cron task created successfully at ", cronExpression);
        } else {
            console.error("Failed to create cron task");
        }
        console.log("Cron job scheduled: Screening will run daily at midnight");
        */
    });
}

main();