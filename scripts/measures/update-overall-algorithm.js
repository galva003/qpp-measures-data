const parse = require('csv-parse/lib/sync');
const _ = require('lodash');
const fs = require('fs');
const path = require('path');


/**
 * `update-overall-algorithm` reads the measureId and overall algorithm
 * fields from a measures CSV file  * then merges the result into the existing
 * set of measures.
 *
 * Example:
 * sh$ staging_measures='../../staging/measures-data.json'
 * sh$ updated_measures='../../util/measures/MIPS_Measures_with_Multiple_Performance_Rates_2017-11-29.csv'
 * node scripts/measures/update-overall-algorithm.js $staging_measures $updated_measures
*/

// TODO(aimee): Add documentation
const mapMeasureId = (measureId) => {
  return measureId.replace(' ', '');
};

// TODO(aimee): Add documentation
const mapOverallAlgorithm = (columnValue) => {
  switch (columnValue) {
    case 'Weighted Average':
      return 'weightedAverage'
      break;
    case 'Simple Average':
      return 'simpleAverage';
      break;
    default:
      return columnValue
      break;
  }
};

/**
 * [config defines how to map values from columns in the origin CSV file]
 * @type {Object}
 *
 *  * `source_fields` are fields which should find values in the CSV input.
 *
 */
const config = {
  sourced_fields: {
    // Needs a function to map 
    measureId: {
      index: 3,
      mappingFunction: mapMeasureId
    },
    overallAlgorithm: {
      index: 4,
      mappingFunction: mapOverallAlgorithm
    }
  }
};

/**
 * [convertCsvToMeasures description]
 * @param  {array of arrays}  records each array in the outer array represents a new measure, each inner array its attributes
 * @param  {object}           config  object defining how to build a new measure from this csv file, including mapping of measure fields to column indices
 * @return {array}            Returns an array of measures objects
 *
 * Notes:
 * 1. The terms [performance rate] 'strata' and 'performance rates' are used interchangeably
 * 2. We trim all data sourced from CSVs because people sometimes unintentionally include spaces or linebreaks
 */
const convertCsvToMeasures = function(records, config) {
  const sourcedFields = config.sourced_fields;

  const newMeasures = records.map(function(record) {
    const newMeasure = {};
    Object.entries(sourcedFields).forEach(function([measureKey, columnObject]) {
      if (typeof columnObject === 'number') {
        if (!record[columnObject]) {
          throw TypeError('Column ' + columnObject + ' does not exist in source data');
        } else {
          // measure data maps directly to data in csv
          newMeasure[measureKey] = _.trim(record[columnObject]);
        }
      } else {
        const mappedValue = columnObject['mappingFunction'](_.trim(record[columnObject.index]));
        newMeasure[measureKey] = mappedValue;
      }
    });

    return newMeasure;
  });

  return newMeasures;
};

// Loads data from csv file and overwrites staging/measures-data.json
function importMeasures(stagingMeasuresDataPath, udpatedMeasuresDataPath) {
  const qpp = fs.readFileSync(path.join(__dirname, stagingMeasuresDataPath), 'utf8');
  const allMeasures = JSON.parse(qpp);

  const csv = fs.readFileSync(path.join(__dirname, udpatedMeasuresDataPath), 'utf8');
  const updateMeasuresCsv = parse(csv, 'utf8');
  // remove header
  updateMeasuresCsv.shift();

  const updatedMeasures = convertCsvToMeasures(updateMeasuresCsv, config);

  //const mergedMeasures = mergeMeasures(allMeasures, qcdrMeasures, outputPath);
  return JSON.stringify(updatedMeasures, null, 2);
}

const stagingMeasuresDataPath = process.argv[2];
const udpatedMeasuresDataPath = process.argv[3];

const newMeasures = importMeasures(stagingMeasuresDataPath, udpatedMeasuresDataPath);
console.log(newMeasures);
// const outputPath = stagingMeasuresDataPath;
// fs.writeFileSync(path.join(__dirname, outputPath), newMeasures);
