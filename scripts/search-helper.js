var papa = require('papaparse');
var fs = require('fs');
var uuidv3 = require('uuid/v3');
var stream = fs.createReadStream("../data/service-data.csv");

var AWS = require('aws-sdk');
AWS.config.loadFromPath('../credentials.json');

var categoryMap = JSON.parse(fs.readFileSync('../output/categoryMap.json', 'utf8'));

const NAMESPACE = "24592ec9-3d6d-4f76-8163-c0d41b50faea";
const SEARCH_HELPER_DOMAIN = "search-helpers-rwq52jv2pag6js3cozrlqwxkry.us-east-1.cloudsearch.amazonaws.com";
const DATABASE_SIZE = 1230;

function addNodes() {
    papa.parse(stream, {
        complete: function (results) {
            var operations = [];

            // ordering is important here
            var dataTypes = ["Service Category", "Macro Category", "Business", "Service"];

            var map = {
                "Service": new Set(),
                "Macro Category": new Set(),
                "Service Category": new Set(),
                "Business": new Set()
            };

            var data = results.data;
            var operation = {
                "type": "add",
                "id": "",
                "fields": {
                    "type": "",
                    "type_order": 0,
                    "name": ""
                }
            };

            for (var i = 0; i < data.length; i++) {

                // console.log(i + " " + data[i]["Service"].trim());
                try {
                    map["Business"] = map["Business"].add(data[i]["Business"].trim());
                    map["Service"] = map["Service"].add(data[i]["Service"].trim());

                    map["Service Category"] = map["Service Category"].add(categoryMap[data[i]["Service"].trim()]["Service Category"]);
                    map["Macro Category"] = map["Macro Category"].add(categoryMap[data[i]["Service"].trim()]["Macro Category"]);
                } catch (err) {
                    console.log("Failed to map categories at i = " + i);
                }
            }

            console.log(map["Service"].size);

            for (var dataType in map) {
                for (let name of map[dataType]) {
                    var copy = JSON.parse(JSON.stringify(operation));
                    copy.fields.name = name;
                    copy.fields.type = dataType;
                    copy.fields.type_order = dataTypes.indexOf(dataType);
                    copy.id = uuidv3(dataType + "|||" + name, NAMESPACE);
                    operations.push(copy);
                }
            }

            fs.writeFile('../output/helper-nodes.json', JSON.stringify(operations), 'utf8', function (err) {
                if (err) {
                    return console.log(err);
                }
                console.log("helper-nodes.json saved");
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
        endpoint: SEARCH_HELPER_DOMAIN
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

            fs.writeFile("../output/delete-search-helpers.json", JSON.stringify(result), function (err) {
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