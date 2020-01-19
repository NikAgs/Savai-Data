var papa = require('papaparse');
var fs = require('fs');
var uuidv3 = require('uuid/v3');
var mapper = require('./helpers');

var AWS = require('aws-sdk');
AWS.config.loadFromPath('../credentials.json');

var stream = fs.createReadStream("../data/service-data.csv");
var geoMap = JSON.parse(fs.readFileSync('../output/geoMap.json', 'utf8'));
var categoryMap = JSON.parse(fs.readFileSync('../output/categoryMap.json', 'utf8'));

const SERVICE_NAMESPACE = "24592ec9-3d6d-4f76-8163-c0d41b50faea";
const BUSINESS_NAMESPACE = "8cc2e530-334e-11ea-abd4-c5adc7570807";
const SERVICE_DOMAIN = "search-services-dvsyeourhhah4hhenkpm2baqoa.us-east-1.cloudsearch.amazonaws.com";

// Change this to the database size
const DATABASE_SIZE = 3035;

function addNodes() {
    papa.parse(stream, {
        complete: function (results) {
            mapper.createBusinessMap(function (map) {
                var operations = [];
                var operation = {
                    "type": "add",
                    "id": "",
                    "fields": {
                        "business_id": "",
                        "name": "",
                        "description": "",
                        "price": 0.0,
                        "price_plus": "false",
                        "discounted_price": 0.0,
                        "business": "",
                        "address": "",
                        "geolocation": "0.0, 0.0",
                        "phone": "",
                        "service_category": "",
                        "macro_category": "",
                        "hours": ""
                    }
                };
                var data = results.data;

                for (var i = 0; i < data.length; i++) {
                    try {
                        var copy = JSON.parse(JSON.stringify(operation));
                        copy.id = uuidv3(data[i].Business.trim() + "|||" + data[i].Service, SERVICE_NAMESPACE);
                        copy.fields.business_id = uuidv3(data[i].Business.trim() + "|||" + map[data[i].Business].Address.trim(), BUSINESS_NAMESPACE);
                        copy.fields.name = data[i].Service;
                        copy.fields.description = data[i].Description.replace(/[^\x20-\x7E]/gmi, "").trim();
                        copy.fields.macro_category = categoryMap[data[i].Service.trim()]["Macro Category"];
                        copy.fields.service_category = categoryMap[data[i].Service.trim()]["Service Category"];
                        copy.fields.discounted_price = parseFloat(data[i]['Discounted Price'].trim().replace(/[^\d.-]/g, '')) || 0.0;
                        copy.fields.price = parseFloat(data[i]['Original Price'].trim().replace(/[^\d.-]/g, ''));
                        copy.fields.price_plus = data[i]['Original Price'].trim().includes("+") ? "true" : "false";
                        copy.fields.business = data[i].Business.trim();
                        copy.fields.address = map[data[i].Business].Address.trim();
                        copy.fields.geolocation = geoMap[data[i].Business];
                        copy.fields.hours = map[data[i].Business].Hours.replace(/[^\x20-\x7E]/gmi, "").trim();

                        if (data[i].Service.trim()) operations.push(copy);
                    } catch (err) {
                        console.log("There was a problem with Service: " + data[i].Service + " from Business: " + data[i].Business);
                    }
                }

                fs.writeFile('../output/service-nodes.json', JSON.stringify(operations), 'utf8', function (err) {
                    if (err) {
                        return console.log(err);
                    }
                    console.log("service-nodes.json saved");
                });
            });

        },
        header: true,
        error: function (err) {
            console.log(err);
        }
    });
}

function deleteNodes() {
    var searchHelpers = new AWS.CloudSearchDomain({
        endpoint: SERVICE_DOMAIN
    });

    var params = {
        query: "lolzcat|-lolzcat",
        queryParser: 'simple',
        size: DATABASE_SIZE
    };

    searchHelpers.search(params, function (err, data) {
        var result = [];
        if (err) {
            console.log("Failed");
            console.log(err);
        } else {
            resultMessage = data;
            for (var i = 0; i < data.hits.hit.length; i++) {
                result.push({
                    "type": "delete",
                    "id": data.hits.hit[i].id
                });
            }

            fs.writeFile("../output/delete-services.json", JSON.stringify(result), function (err) {
                if (err) {
                    return console.log(err);
                }
                console.log("The file was saved!");
            });
        }
    });
}

//deleteNodes();
addNodes();