const fs = require("fs").promises;
const { MongoClient } = require("mongodb");
const ibfReader = require("ibf-file-reader");

(async () => {
  const dbs = await connectToDB();

  // await deleteMongoData({ bg, treatments, deviceStatus });
  const CUT_OFF_TIME = await getCutOffTime();

  await deleteResultFile();

  const records = { blood_glucose: [], bolus: [], carb: [], activate: [], deactivate: [], download: [] };
  await ibfReader(`${__dirname}/dump.ibf`, handleRecord.bind(null, records, CUT_OFF_TIME));

  //await saveRecord({ bg, treatments, deviceStatus }, records);

  console.log("Done");
  process.exit();
})();

async function handleRecord(records, CUT_OFF_TIME, { recordType, record }) {
  if (ignoredRecordTypes.includes(recordType)) {
    return;
  }
  record.timestamp = convertTime(record);
  if (record.timestamp <= CUT_OFF_TIME) {
    return;
  }
  recordType = recordType.toLowerCase();
  record = cleanRecord({ recordType, record });
  record.pdm = true;

  if (["download", "activate", "deactivate"].includes(recordType)) {
    record.type = recordType;
  }

  records[recordType].push(record);

  await fs.appendFile(`${__dirname}/result.json`, JSON.stringify(record) + "\n");
}

async function saveRecord({ bg, treatments, deviceStatus }, records) {
  const blood_glucose = records.blood_glucose.map(cleanBG);
  const bolus = records.bolus.map(cleanBolus);
  const { carb: carbs, activate, deactivate, download } = records;
  const bolusCarbs = [...bolus, ...carbs];
  const statuses = [...activate, ...deactivate, ...download];
  try {
    blood_glucose.length && (await bg.insertMany(blood_glucose));
    bolusCarbs.length && (await treatments.insertMany(bolusCarbs));
    statuses.length && (await deviceStatus.insertMany(statuses));
  } catch (e) {
    console.error(e);
  }
}

function cleanBG({ timestamp: date, bgReading: svg, pdm }) {
  return { date, svg, pdm };
}

function cleanBolus({ timestamp, units: insulin, extendedDurationMinutes, pdm }) {
  const prop = { timestamp, insulin, extendedDurationMinutes, pdm };
  if (!extendedDurationMinutes) {
    delete prop.extendedDurationMinutes;
  }
  return prop;
}

function cleanRecord({ recordType, record }) {
  let props = [...propertiesToRemove.common, ...(propertiesToRemove[recordType] || [])];
  for (let prop of props) {
    delete record[prop];
  }
  for (let prop of propertiesToRemoveIfUnused[recordType] || []) {
    if (record[prop] == 0) {
      delete record[prop];
    }
  }
  return Object.assign({}, record);
}

function convertTime({ timestamp }) {
  return new Date(timestamp).getTime();
}

async function deleteResultFile() {
  await fs.writeFile(`${__dirname}/result.json`, "");
}

async function deleteMongoData({ bg, treatments, deviceStatus }) {
  await bg.deleteMany({ pdm: true });
  await treatments.deleteMany({ pdm: true });
  await deviceStatus.deleteMany({ pdm: true });
}

async function connectToDB() {
  const credentials = (await fs.readFile(`${__dirname}/secrets`, "utf-8")).trim();
  const uri = `mongodb+srv://${credentials}@cluster0-xtbt8.mongodb.net/cgm?retryWrites=true&w=majority`;

  const db = await MongoClient.connect(uri, { useUnifiedTopology: true });
  const cgm = db.db("cgm");
  const bg = cgm.collection("bg");
  const treatments = cgm.collection("treatments");
  const deviceStatus = cgm.collection("devicestatus");

  return { bg, treatments, deviceStatus };
}
async function getCutOffTime() {
  const [{ timestamp: CUT_OFF_TIME }] = await deviceStatus
    .find({ type: "download" }, { projection: { _id: 0 } })
    .sort({ timestamp: -1 })
    .limit(1)
    .toArray();
  return CUT_OFF_TIME;
}

const ignoredRecordTypes = [
  "BASAL_RATE",
  "SUGGESTED_CALC",
  "TERMINATE_BOLUS",
  "TIME_CHANGE",
  "DATE_CHANGE",
  "REMOTE_HAZARD_ALARM",
  "RESUME",
  "SUSPEND",
  "TERMINATE_BASAL",
  "END_MARKER",
  "PUMP_ALARM"
];
const propertiesToRemove = {
  common: ["error", "logType", "logIndex", "secondsSincePowerUp", "historyLogRecordType", "flags"],
  bolus: ["calculationRecordOffset", "immediateDurationSeconds", "extended"],
  blood_glucose: ["errorCode", "userTag1", "userTag2", "bgFlags"],
  carb: ["wasPreset", "presetType"],
  activate: ["lotNumber", "serialNumber", "podVersion", "interlockVersion"]
};
const propertiesToRemoveIfUnused = {
  bolus: ["extendedDurationMinutes"]
};
