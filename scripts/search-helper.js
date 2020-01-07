var papa = require('papaparse');
var fs = require('fs');
var uuidv3 = require('uuid/v3');
var stream = fs.createReadStream("../data/service-data.csv");

const NAMESPACE = "24592ec9-3d6d-4f76-8163-c0d41b50faea";

papa.parse(stream, {
    complete: function (results) {
        var operations = [];
        // ordering is important here
        var dataTypes = ["Service Category", "Macro Category", "Micro Category", "Location", "Service"];
        console.log(dataTypes);
        var map = {
            "Service": new Set(),
            "Micro Category": new Set(),
            "Macro Category": new Set(),
            "Service Category": new Set(),
            "Location": new Set()
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
            for (var j = 0; j < dataTypes.length; j++) {
                map[dataTypes[j]] = map[dataTypes[j]].add(data[i][dataTypes[j]].trim());
            }
        }

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

// Data needed for search helpers
// - Service
// - Micro Category
// - Macro Category
// - Service Category 
// - Location

// Sample query:
// man*
// format: {SEARCH TEXT}*