const moment = require('moment');
const execSync = require("child_process").execSync;

// Your Google Cloud Platform project ID
const bqProjectId = 'rva-media-library-test';
// const bqProjectId = 'avid-life-623';

// The name for the new dataset
const bqFromDataset = 'RiseStorageLogsTest';
// const bqFromDataset = 'RiseStorageLogs';

const bqToDataset = 'test_dataset';
// const bqToDataset = 'RiseStorageLogsTest_v2';
// const bqToDataset = 'RiseStorageLogs_v2';

const bqFromTable = 'StorageLogs';

const bqToTable = 'StorageLogs';


const fromDate = moment('2015-03-01');
const toDate = moment('2016-01-01');

const processStartTime = Date.now();

var tableCount = 0;
var processedTables = [];

console.log(`Setting default GCloud project: ${bqProjectId}`);
execSync(`gcloud config set project '${bqProjectId}'`);

for (var batchDay = moment(fromDate); batchDay.isBefore(toDate); batchDay.add(1, 'months')) {
  var sourceTableCurrent = batchDay.format('YYYY_M');

  var sourceTable = `[${bqFromDataset}.${bqFromTable}${sourceTableCurrent}]`;
  var destinationTable = `[${bqToDataset}.${bqToTable}${batchDay.format('YYYYMM')}01]`;

  var tableStartTime = Date.now();

  var bqCopyCommand = `bq cp ${sourceTable} ${destinationTable}`;

  console.log(`Beginning process for ${sourceTableCurrent}`);

  var pt;
  try {
    _runCommand(bqCopyCommand, `Running batch command`);
    
    pt = { table: sourceTableCurrent, time: (Date.now() - tableStartTime) };
  } catch (err) {
    console.log(`Table error ${sourceTableCurrent}`, err);

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
