var fs = require('fs');
var papa = require('papaparse');
var async = require('async');
var uuidv3 = require('uuid/v3');

var stream = fs.createReadStream("../data/business-data.csv");

var AWS = require('aws-sdk');
AWS.config.loadFromPath('../credentials.json');

var service_nodes = JSON.parse(fs.readFileSync('../output/service-nodes.json', 'utf8'));

const dynamodbDocClient = new AWS.DynamoDB({
    region: "us-west-1"
});

const NAMESPACE = "8cc2e530-334e-11ea-abd4-c5adc7570807";
const BUCKET_NAME = 'savai-business-images';

/*
 * MUST HAVE UPDATED service-nodes.json IN OUTPUT BEFORE RUNNING!!
 */
papa.parse(stream, {
    complete: async function (results) {
        var data = results.data;
        var split_arrays = [];
        var size = 25;

        while (data.length > 0) {
            let cur25 = data.splice(0, size);
            let item_data = [];

            for (var i = cur25.length - 1; i >= 0; i--) {
                var this_item = {
                    "PutRequest": {
                        "Item": {}
                    }
                };

                if (cur25[i].Business.length > 0 && cur25[i].Address.length > 0) {
                    this_item.PutRequest.Item.business = {
                        "S": cur25[i].Business.trim()
                    };
                    this_item.PutRequest.Item.address = {
                        "S": cur25[i].Address.trim()
                    };
                    this_item.PutRequest.Item.business_id = {
                        "S": uuidv3(cur25[i].Business.trim() + "|||" + cur25[i].Address.trim(), NAMESPACE)
                    };
                } else {
                    continue;
                }

                if (cur25[i]["Phone Number"].trim().length > 0) this_item.PutRequest.Item.phone_number = {
                    "S": cur25[i]["Phone Number"].trim()
                };
                if (cur25[i]["Service Pricing Tracker"].trim().length > 0) this_item.PutRequest.Item.price_tracker = {
                    "S": cur25[i]["Service Pricing Tracker"].trim()
                };
                if (cur25[i].Photo.trim().length > 0) this_item.PutRequest.Item.photo = {
                    "S": cur25[i].Photo.trim()
                };
                if (cur25[i].Notes.trim().length > 0) this_item.PutRequest.Item.notes = {
                    "S": cur25[i].Notes.trim()
                };
                if (cur25[i].District.trim().length > 0) this_item.PutRequest.Item.district = {
                    "S": cur25[i].District.trim()
                };
                if (cur25[i].Hours.trim().length > 0) this_item.PutRequest.Item.hours = {
                    "S": cur25[i].Hours.trim()
                };

                // Add services from this business as a JSON array
                var services = [];
                for (var j = 0; j < service_nodes.length; j++) {
                    if (service_nodes[j].fields.business_id == uuidv3(cur25[i].Business.trim() + "|||" + cur25[i].Address.trim(), NAMESPACE)) {
                        services.push(service_nodes[j].fields);
                    }
                }
                this_item.PutRequest.Item.services = {
                    "S": JSON.stringify(services)
                };

                // Add photo URLs if they exist in S3
                try {
                    await new AWS.S3().headObject({
                        Bucket: BUCKET_NAME,
                        Key: uuidv3(cur25[i].Business.trim() + "|||" + cur25[i].Address.trim(), NAMESPACE) + "-1"
                    }).promise();

                    this_item.PutRequest.Item.photos = {
                        "S": JSON.stringify(["https://savai-business-images.s3-us-west-1.amazonaws.com/" + uuidv3(cur25[i].Business.trim() + "|||" + cur25[i].Address.trim(), NAMESPACE) + "-1"])
                    };
                } catch (err) {}

                //console.log(JSON.stringify(this_item));
                item_data.push(this_item);
            }

            if (item_data.length > 0) {
                split_arrays.push(item_data);
            }
        }
        data_imported = false;
        chunk_no = 1;
        async.each(split_arrays, (item_data, callback) => {
            const params = {
                RequestItems: {
                    "Business_Data": item_data
                }
            }
            dynamodbDocClient.batchWriteItem(params, function (err, res, cap) {
                if (err === null) {
                    console.log('Success chunk #' + chunk_no);
                    data_imported = true;
                } else {
                    console.log(err);
                    console.log('Fail chunk #' + chunk_no);
                    data_imported = false;
                }
                chunk_no++;
                callback();
            });

        }, () => {
            // run after loops
            console.log('all data imported....');

        });
    },
    header: true,
    error: function (err) {
        console.log(err);
    }
});