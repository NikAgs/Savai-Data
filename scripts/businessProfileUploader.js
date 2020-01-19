var fs = require('fs');
var papa = require('papaparse');
var async = require('async');
var uuidv3 = require('uuid/v3');

var stream = fs.createReadStream("../data/business-data.csv");

var AWS = require('aws-sdk');
AWS.config.loadFromPath('../credentials.json');

const dynamodbDocClient = new AWS.DynamoDB({
    region: "us-west-1"
});

const NAMESPACE = "8cc2e530-334e-11ea-abd4-c5adc7570807";

papa.parse(stream, {
    complete: function (results) {
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

                if (cur25[1].Business.length > 0 && cur25[1].Address.length > 0) {
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