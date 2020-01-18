var papa = require('papaparse');
var fs = require('fs');
var NodeGeocoder = require('node-geocoder');
var stream = fs.createReadStream("../data/business-data.csv");

module.exports = {
    createBusinessMap: function (callback) {
        papa.parse(stream, {
            complete: function (results) {
                var data = results.data;
                var map = {};

                for (var i = 0; i < data.length; i++) {
                    if (data[i].Business && data[i].Address) {
                        map[data[i].Business] = {
                            "Address": data[i].Address,
                            "Hours": data[i].Hours
                        };
                    }
                }

                callback(map);
            },
            header: true,
            error: function (err) {
                console.log(err);
            }
        });
    },
    getGeolocations: function () {
        module.exports.createBusinessMap(function (map) {
            var geocoderOptions = {
                provider: 'google',
                httpAdapter: 'https',
                apiKey: 'AIzaSyB4xfretOLFvLYe3Fs6V9-I0lPtUVcPFmY',
                formatter: null
            };
            var geocoder = NodeGeocoder(geocoderOptions);
            var addresses = [];
            var businesses = [];

            for (var business in map) {
                addresses.push(map[business].Address);
                businesses.push(business);
            }

            geocoder.batchGeocode(addresses, function (err, results) {
                var i = 0;
                var geoMap = JSON.parse(fs.readFileSync('../output/geoMap.json', 'utf8'));

                // Had to read and append to current file because of inconsistent responses from Google API
                results.forEach(function (result) {
                    try {
                        geoMap[businesses[i++]] = result.value[0].latitude + ", " + result.value[0].longitude;
                    } catch (err) {
                        if (!geoMap[businesses[i - 1]]) {
                            console.log("Couldnt map business: " + businesses[i - 1]);
                        }
                    }
                });

                console.log("Succesfully mapped: " + Object.keys(geoMap).length + "/" + businesses.length);
                fs.writeFile('../output/geoMap.json', JSON.stringify(geoMap), 'utf8', function (err) {
                    if (err) {
                        return console.log(err);
                    }
                    console.log("geoMap.json saved");
                });

                return geoMap;
            });
        });
    }
};