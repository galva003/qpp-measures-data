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
 * updated_measures='../../util/measures/2017_QPP_Individual_Measure_eMeasure_Tags-112916.csv'
 * node scripts/measures/update-overall-algorithm.js $staging_measures $updated_measures
*/

// TODO(aimee): Add documentation
const mapMeasureId = (measureId) => {
  const measureIdString = measureId.toString();
  return measureIdString.padStart(3, '0');
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
    eMeasureId: 1,
    // Needs a function to map 
    measureId: {
      index: 0,
      mappingFunction: mapMeasureId
    },
    overallAlgorithm: {
      index: 9,
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

  const newMeasures = records.map((record, idx) => {
    const newMeasure = {};
    Object.entries(sourcedFields).forEach(([measureKey, columnObject]) => {
      if (typeof columnObject === 'number') {
        newMeasure[measureKey] = _.trim(record[columnObject]);
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
  updatedMeasures.forEach((updatedMeasure, idx) => {
    const existingMeasure = existingMeasures.find((measure) => {
      return measure.measureId === updatedMeasure.measureId;
    });

    // Skip any measures which don't already exist in measures-data.json
    if (!existingMeasure) {
      const explanation = 'measure doesn\'t exist in measures-data';
      resultsString += updatedMeasure.measureId + ',' + explanation + '\n';
      return;
    };

    // First case: aggregate algorithms are expected
    if (['simpleAverage', 'weightedAverage'].includes(updatedMeasure.overallAlgorithm)) {
      if (existingMeasure.overallAlgorithm !== updatedMeasure.overallAlgorithm) {
        const explanation = 'Making a change to: ' + existingMeasure.measureId;
        resultsString += updatedMeasure.measureId + ',' + explanation + '\n';
      } else {
        const explanation = 'No change required for: ' + existingMeasure.measureId;
        resultsString += updatedMeasure.measureId + ',' + explanation + '\n';
      }
      existingMeasure.overallAlgorithm = updatedMeasure.overallAlgorithm;
    } else {
      // Second case: The overall algorithm is identified as an ordinal of the performance rates
  
      // Determine the position of the overall algorithm's performance rate, indexed starting starting from 1.
      const stratumPostionMatch = updatedMeasure.overallAlgorithm
        .match(/([0-9]{1,})(st|nd|rd|th) Performance Rate/);

      // If it's not a performance rate ordinal and it's not 'N/A' it's special somehow...
      if (!stratumPostionMatch && (updatedMeasure.overallAlgorithm !== 'N/A')) {
        const explanation = 'Found unexpected overallAlgorithm: ' + updatedMeasure.overallAlgorithm + '\n';
        resultsString += updatedMeasure.measureId + ',' + explanation;
        return;
      } else {
        if (existingMeasure.overallAlgorithm !== undefined) {
          const explanation = 'existing measures overallAlgorithm is ' + existingMeasure.overallAlgorithm + ', should be N/A\n';
          resultsString += updatedMeasure.measureId + ',' + explanation;
        }
        return;
      }

      if (stratumPostionMatch) {
        const stratumPostion = stratumPostionMatch[1];

        // If it's a performance rate ordinal, check it has a strata array
        if (!Array.isArray(existingMeasure.strata)) {
          const explanation = 'Updated measure specifies overall performance rate should be ' + updatedMeasure.overallAlgorithm +
             ' but existing measure: ' + existingMeasure.measureId + ' has no strata (' + existingMeasure.metricType + ').';
          resultsString += updatedMeasure.measureId + ',' + explanation + '\n';
          return;
        }

        // If it's a performance rate ordinal and it has a strata array, check
        // that the ordinal specified is the "overall" stratum.
        const existingStratum = existingMeasure.strata[stratumPostion - 1];
        if (existingStratum.name === "overall") {
          if (existingMeasure.overallAlgorithm !== "overallStratumOnly") {
            const explanation = 'Making a change to: ' + existingMeasure.measureId;
            resultsString += updatedMeasure.measureId + ',' + explanation + '\n';
          } else {
            const explanation = 'No change required for: ' + existingMeasure.measureId;
            resultsString += updatedMeasure.measureId + ',' + explanation + '\n';
          }        
          existingMeasure.overallAlgorithm = "overallStratumOnly";
        } else {
          // If it isn't the overall stratum, notice that.
          const explanation = 'Strata identified is not the overall strata in existing measures data for measure: ' + existingMeasure.measureId +
            '. Existing stratum: ' + existingStratum.name +
            '. Updated measure algorithm: ' + updatedMeasure.overallAlgorithm;
          resultsString += updatedMeasure.measureId + ',' + explanation + '\n';
        }
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
  console.log(resultsString);
  //return JSON.stringify(measuresWithUpdatedOverallAlgorithms, null, 2);
}

const stagingMeasuresDataPath = process.argv[2];
const udpatedMeasuresDataPath = process.argv[3];

const updatedMeasures = importMeasures(stagingMeasuresDataPath, udpatedMeasuresDataPath);
// fs.writeFileSync(path.join(__dirname, 'results.csv'), updatedMeasures);
