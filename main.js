const moment = require('moment');
const execSync = require("child_process").execSync;

// Your Google Cloud Platform project ID
// const bqProjectId = 'rva-media-library-test';
const bqProjectId = 'avid-life-623';

// The name for the new dataset
// const bqFromDataset = 'RiseStorageLogsTest';
const bqFromDataset = 'RiseStorageLogs';

// const bqToDataset = 'test_dataset';
// const bqToDataset = 'RiseStorageLogsTest_v2';
const bqToDataset = 'RiseStorageLogs_v2';

const bqFromTable = 'UsageLogs';

const bqToTable = 'UsageLogs';

// const DAY_MS = 24 * 60 * 60 * 1000;

// if(process.argv.length != 4) {
//   console.log("You must provide startDate and tableCount");
//   return;
// }

// const fromDate = moment('2015-01-01');
// const toDate = moment('2017-07-01');

const fromDate = moment('2015-03-01');
const toDate = moment('2015-04-01');

const processStartTime = Date.now();
// var dateFrom = process.argv[2] ? new Date(process.argv[2]) : new Date(2017, 02, 22);
// var tableCount = process.argv[3] ? Number(process.argv[3]) : 1;
// var processedTables = [];

var tableCount = 0;
var processedTables = [];

console.log(`Setting default GCloud project: ${bqProjectId}`);
execSync(`gcloud config set project '${bqProjectId}'`);

for (var batchDay = moment(fromDate); batchDay.isBefore(toDate); batchDay.add(1, 'days')) {
  var batchNextDay = batchDay.clone().add(1, 'days');
  var batchNextMonth = batchDay.clone().add(1, 'months');
  var batchFromDate = batchDay.format('YYYY-MM-DD');
  var batchToDate = batchNextDay.format('YYYY-MM-DD');

  var sourceTableCurrent = batchDay.format('YYYY_M');
  var sourceTableNext = batchNextMonth.format('YYYY_M');

  var sourceTable = `TABLE_QUERY([${bqFromDataset}], \\"table_id = '${bqFromTable}${sourceTableCurrent}' OR table_id = '${bqFromTable}${sourceTableNext}'\\")`;
  var destinationTable = `${bqToDataset}.${bqToTable}${batchDay.format('YYYYMMDD')}`;

  var tableStartTime = Date.now();

  var bqExportCommand = `bq query --format=none --destination_table '${destinationTable}' --allow_large_results "SELECT * FROM ${sourceTable} where time_micros >= TIMESTAMP('${batchFromDate}') and time_micros < TIMESTAMP('${batchToDate}')"`;

  console.log(`Beginning process for ${batchFromDate}`);

  var pt;
  try {
    _runCommand(bqExportCommand, `Running batch command`);
    
    pt = { table: batchFromDate, time: (Date.now() - tableStartTime) };
  } catch (err) {
    console.log(`Table error ${batchFromDate}`, err);

    pt = { table: batchFromDate, time: (Date.now() - tableStartTime), error: true };
  }

  processedTables.push(pt);
  _printTableData(pt);
}

console.log(`Finished process, total time: ${_tsToSec(Date.now() - processStartTime)} secs (${_tsToMin(Date.now() - processStartTime)} mins)`);

processedTables.forEach(_printTableData);

function _runCommand(command, label) {
  console.log(label, command);
  execSync(command, { stdio:[0,1,2] });
}

function _printTableData(pt) {
  console.log(`Table: ${pt.table}, total time: ${_tsToSec(pt.time)} secs (${_tsToMin(pt.time)} mins) ${pt.error?'Failed':'Successful'}`);
}

function _formatDecimal(number) {
  return Math.floor(number * 100) / 100;
}

function _tsToSec(ts) {
  return _formatDecimal(ts / 1000);
}

function _tsToMin(ts) {
  return _formatDecimal(ts / 1000 / 60);
}
