var AWS = require('aws-sdk');
var fs = require('fs');
AWS.config.loadFromPath('../credentials.json');
const service_nodes = JSON.parse(fs.readFileSync('../output/service-nodes.json', 'utf8'));

const BUCKET_NAME = 'savai-business-images';

var allKeys = [];

/*
 * Renames all the s3 images from Sarina's formatting with business names to indexed business_id's
 * 
 * Note: service-nodes.json must be up-to-date
 */
listAllKeys(null, async () => {
    for (var i = 0; i < allKeys.length; i++) {
        var business = removeExtentions(allKeys[i].Key);
        const regex = / [0-9]/;
        var imageNumber;

        if (regex.test(business.slice(-2))) {
            imageNumber = business.slice(-1);
            business = business.substring(0,business.length - 2);
        } else {
            console.log("Couldn't parse image number from: " + business);
            continue;
        }

        for (var j = 0; j < service_nodes.length; j++) {
            if (service_nodes[j].fields.business == business) {
                try {
                    await renameImage(allKeys[i].Key, service_nodes[j].fields.business_id + "-" + imageNumber);
                    console.log("Renamed " + business + " to " + service_nodes[j].fields.business_id + "-" + imageNumber);
                } catch (err) {
                    console.log("There was a problem with " + business);
                    console.log(err);
                }
                break;
            }
        }
    }
});

function listAllKeys(token, cb) {
    var opts = {
        Bucket: BUCKET_NAME
    };
    if (token) opts.ContinuationToken = token;

    new AWS.S3().listObjectsV2(opts, function (err, data) {
        allKeys = allKeys.concat(data.Contents);

        if (data.IsTruncated)
            listAllKeys(data.NextContinuationToken, cb);
        else
            cb();
    });
}

async function renameImage(oldName, newName) {
    // Copy the object to a new location
    await new AWS.S3().copyObject({
        Bucket: BUCKET_NAME,
        CopySource: `${BUCKET_NAME}/${oldName}`,
        Key: newName,
        ACL: "public-read"
    }).promise();

    await new AWS.S3().deleteObject({
        Bucket: BUCKET_NAME,
        Key: oldName
    }).promise();
}

function removeExtentions(imageName) {
    return imageName
        .replace("_", "&")
        .replace(".JPG", "")
        .replace(".jpg", "")
        .replace(".png", "")
        .replace(".PNG", "")
        .replace(".webp", "")
        .replace(".WEBP", "");
}