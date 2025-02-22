// ------------------------------------------------------------------------------------
// Populate collName with the time-series collection that failed validation due
// to v2/v3 timeseries buckets not in correct sorted/unsorted order
// respectively.
// ------------------------------------------------------------------------------------
const collName = 'your_collection_name';

let listCollectionsRes = db.runCommand({
                             listCollections: 1.0,
                             filter: {name: collName}
                           }).cursor.firstBatch;
if (listCollectionsRes.length == 0) {
  print(
      'Collection not found. Populate collName with the time-series collection that failed due to v2/v3 timeseries buckets not in correct sorted/unsorted order respectively.');
  exit(1);
}
const coll = db.getCollection(collName);

//
// NON-MODIFIABLE CODE BELOW
//
// ---------------------------------------------------------------------------------------
// The script will, for each bucket in the affected time-series collection:
// 1) Detect if the bucket has bucket version mismatch.
// 2) Change the buckets with bucket version mismatch to the correct version.
// 3) Validate that there are no bucket version mismatches.
// ----------------------------------------------------------------------------------------

BucketVersion = {
  kCompressedSorted: 2,
  kCompressedUnsorted: 3
};

function bucketHasMismatchedBucketVersion(
    bucketsColl, bucketId, bucketControlVersion) {
  let measurements = bucketsColl
                         .aggregate([
                           {$match: {_id: bucketId}}, {
                             $_unpackBucket: {
                               timeField: 't',
                             }
                           }
                         ])
                         .toArray();
  let prevTimestamp = new Date(-8640000000000000);
  let detectedOutOfOrder = false;
  for (let i = 0; i < measurements.length; i++) {
    let currMeasurement = measurements[i]['t'];
    let currTimestamp = new Date(currMeasurement);
    if (currTimestamp < prevTimestamp) {
      if (bucketControlVersion == BucketVersion.kCompressedSorted) {
        return true;
      } else if (bucketControlVersion == BucketVersion.kCompressedUnsorted) {
        detectedOutOfOrder = true;
      }
    }
    prevTimestamp = currTimestamp;
  }
  return !detectedOutOfOrder &&
      (bucketControlVersion == BucketVersion.kCompressedUnsorted);
}

function runFixBucketVersionMismatchProcedure(collName) {
  print(
      'Checking if the bucket versions match their data in ' + collName +
      ' ...\n');
  // Range through all the bucketDocs and change the control version of the
  // bucket from 2 -> 3 if the data is not sorted or from 3 -> 2 if the data is
  // sorted.
  const bucketsColl = db.getCollection('system.buckets.' + coll.getName());
  var cursor = bucketsColl.find({});

  while (cursor.hasNext()) {
    const bucket = cursor.next();
    const bucketId = bucket._id;
    const bucketControlVersion = bucket.control.version;
    if (bucketHasMismatchedBucketVersion(
            bucketsColl, bucketId, bucketControlVersion)) {
      if (bucketControlVersion == BucketVersion.kCompressedSorted) {
        assert.commandWorked(bucketsColl.updateOne(
            {_id: bucketId},
            {$set: {'control.version': BucketVersion.kCompressedUnsorted}}));
      } else if (bucketControlVersion == BucketVersion.kCompressedUnsorted) {
        assert.commandWorked(bucketsColl.updateOne(
            {_id: bucketId},
            {$set: {'control.version': BucketVersion.kCompressedSorted}}));
      }
    }
  }
}

//
// Steps 1 & 2: Detect if the bucket has bucket version mismatch and change the
// buckets with bucket version mismatch to the correct version.
//
runFixBucketVersionMismatchProcedure(collName);

//
// Step 3: Validate that there are no bucket version mismatches.
//
print('Validating that there are no mismatched bucket versions ...\n');
db.getMongo().setReadPref('secondaryPreferred');
const validateRes = collName.validate({background: true});

//
// For v8.1.0+, buckets that have a bucket version mismatch will lead to a error
// during validation.
//
// Prior to v8.1.0, buckets that have a bucket version mismatch will lead to a
// warning during validation.
//
if ((validateRes.errors.length != 0 &&
     validateRes.errors.some(x => x.includes('6698300'))) ||
    (validateRes.warnings.length != 0 &&
     validateRes.warnings.some(x => x.includes('6698300')))) {
  print(
      '\nThere is still a time-series bucket(s) that has a bucket version mismatch, or there is another error or warning during validation regarding incompatible time-series documents. Check logs with id 6698300.');
  exit(1);
}

print('\nScript successfully fixed mismatched bucket versions!');
exit(0);
