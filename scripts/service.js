var papa = require('papaparse');
var fs = require('fs');
var uuidv3 = require('uuid/v3');

var mapper = require('./helpers');
var stream = fs.createReadStream("../data/service-data (old).csv");
var geoMap = JSON.parse(fs.readFileSync('../output/geoMap.json', 'utf8'));
const NAMESPACE = "24592ec9-3d6d-4f76-8163-c0d41b50faea";

papa.parse(stream, {
    complete: function (results) {
        mapper.createAddressMap(function (map) {
            var operations = [];
            var operation = {
                "type": "add",
                "id": "",
                "fields": {
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
                    "micro_category": "",
                    "macro_category": "",
                    "profile_url": "",
                    "hours": ""
                }
            };
            var data = results.data;

            for (var i = 0; i < data.length; i++) {
                var copy = JSON.parse(JSON.stringify(operation));
                copy.id = uuidv3(data[i].Location.trim() + "|||" + data[i].Service, NAMESPACE);
                copy.fields.name = data[i].Service;
                copy.fields.description = data[i].Description.trim();
                copy.fields.micro_category = data[i]['Micro Category'].trim();
                copy.fields.macro_category = data[i]['Macro Category'].trim();
                copy.fields.service_category = data[i]['Service Category'].trim();
                copy.fields.discounted_price = parseFloat(data[i]['Discounted Price'].trim().replace(/[^\d.-]/g, '')) || 0.0;
                copy.fields.price = parseFloat(data[i]['Original Price'].trim().replace(/[^\d.-]/g, ''));
                copy.fields.price_plus = data[i]['Original Price'].trim().includes("+") ? "true" : "false";
                copy.fields.business = data[i].Location.trim();
                copy.fields.address = map.get(data[i].Location);
                copy.fields.geolocation = geoMap[map.get(data[i].Location)];
                copy.fields.hours = data[i].Hours.trim();

                operations.push(copy);
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