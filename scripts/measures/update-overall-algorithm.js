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
 * staging_measures='../../measures/measures-data.json'
 * updated_measures='../../util/measures/MIPS_Measures_with_Multiple_Performance_Rates_2017-11-29.csv'
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
      // if we can't map the column directly, it's some ordinal representation
      // of overall stratum that we will verify when merging with measures
      // data.
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
    ogMeasureId: 3,
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

// TODO(aimee): Add documentation
function mergeMeasures(existingMeasures, updatedMeasures) {
  let resultsString = '';
  updatedMeasures.forEach((updatedMeasure) => {
    const existingMeasure = existingMeasures.find((measure) => {
      return measure.measureId === updatedMeasure.measureId;
    });

    // Skip any measures which don't already exist in measures-data.json
    if (!existingMeasure) {
      const explanation = 'measure doesn\'t exist in measures-data';
      resultsString += updatedMeasure.ogMeasureId + ',' + explanation + '\n';
      return;
    };

    if (['simpleAverage', 'weightedAverage'].includes(updatedMeasure.overallAlgorithm)) {
      if (existingMeasure.overallAlgorithm !== updatedMeasure.overallAlgorithm) {
        const explanation = 'Making a change to: ' + existingMeasure.measureId;
        resultsString += updatedMeasure.ogMeasureId + ',' + explanation + '\n';
      } else {
        const explanation = 'No change required for: ' + existingMeasure.measureId;
        resultsString += updatedMeasure.ogMeasureId + ',' + explanation + '\n';
      }
      existingMeasure.overallAlgorithm = updatedMeasure.overallAlgorithm;
    } else {
      // position of the assignable performance rate, indexed starting starting from 1.
      const stratumPostion = updatedMeasure.overallAlgorithm
        .match(/([0-9]{1,})(st|nd|rd|th) Performance Rate/)[1];
      if (!Array.isArray(existingMeasure.strata)) {
        const explanation = 'Updated measure specifies overall performance rate should be ' + updatedMeasure.overallAlgorithm +
           ' but existing measure: ' + existingMeasure.measureId + ' has no strata (' + existingMeasure.metricType + ').';
        resultsString += updatedMeasure.ogMeasureId + ',' + explanation + '\n';
        return;
      }
      const existingStratum = existingMeasure.strata[stratumPostion - 1];
      if (existingStratum.name === "overall") {
        if (existingMeasure.overallAlgorithm !== "overallStratumOnly") {
          const explanation = 'Making a change to: ' + existingMeasure.measureId;
          resultsString += updatedMeasure.ogMeasureId + ',' + explanation + '\n';
        } else {
          const explanation = 'No change required for: ' + existingMeasure.measureId;
          resultsString += updatedMeasure.ogMeasureId + ',' + explanation + '\n';
        }        
        existingMeasure.overallAlgorithm = "overallStratumOnly";
      } else {
        const explanation = 'Strata identified is not the overall strata in existing measures data for measure: ' + existingMeasure.measureId +
          '. Existing stratum: ' + existingStratum.name +
          '. Updated measure algorithm: ' + updatedMeasure.overallAlgorithm;
        resultsString += updatedMeasure.ogMeasureId + ',' + explanation + '\n';
      }
    }
  });

  // return existingMeasures;
  return resultsString;
};

// Loads data from csv file and overwrites staging/measures-data.json
function importMeasures(stagingMeasuresDataPath, udpatedMeasuresDataPath) {
  const qpp = fs.readFileSync(path.join(__dirname, stagingMeasuresDataPath), 'utf8');
  const existingMeasures = JSON.parse(qpp);

  const csv = fs.readFileSync(path.join(__dirname, udpatedMeasuresDataPath), 'utf8');
  const updateMeasuresCsv = parse(csv, 'utf8');
  // remove header
  updateMeasuresCsv.shift();

  const updatedMeasures = convertCsvToMeasures(updateMeasuresCsv, config);
  // const measuresWithUpdatedOverallAlgorithms = mergeMeasures(existingMeasures, updatedMeasures);
  const resultsString = mergeMeasures(existingMeasures, updatedMeasures);
  return resultsString;
  //return JSON.stringify(measuresWithUpdatedOverallAlgorithms, null, 2);
}

const stagingMeasuresDataPath = process.argv[2];
const udpatedMeasuresDataPath = process.argv[3];

const updatedMeasures = importMeasures(stagingMeasuresDataPath, udpatedMeasuresDataPath);
fs.writeFileSync(path.join(__dirname, 'results.csv'), updatedMeasures);
