var papa = require('papaparse');
var fs = require('fs');
var stream = fs.createReadStream("../data/category-map.csv");

function createCategoryMap() {
    var map = {};

    papa.parse(stream, {
        complete: function (results) {
            const data = results.data;

            for (let i = 0; i < data.length; i++) {

                const service = data[i]["Service"];
                map[service] = {
                    "Macro Category": data[i]["Macro Category"],
                    "Service Category": data[i]["Service Category"]
                };
            }

            fs.writeFile('../output/categoryMap.json', JSON.stringify(map), 'utf8', function (err) {
                if (err) {
                    return console.log(err);
                }
                console.log("categoryMap.json saved");
            });
        },
        header: true,
        error: function (err) {
            console.log(err);
        }
    });
}

createCategoryMap();