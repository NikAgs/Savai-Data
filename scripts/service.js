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
const SERVICE_DOMAIN = "search-services-cvfpbrz6fmcwfwrssmlmvfr7fu.us-west-1.cloudsearch.amazonaws.com";
const DOCUMENT_DOMAIN = "doc-services-cvfpbrz6fmcwfwrssmlmvfr7fu.us-west-1.cloudsearch.amazonaws.com";

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
                        "hours_text": "",
                        "monday_hours": [],
                        "tuesday_hours": [],
                        "wednesday_hours": [],
                        "thursday_hours": [],
                        "friday_hours": [],
                        "saturday_hours": [],
                        "sunday_hours": []
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
                        copy.fields.hours_text = map[data[i].Business].Hours.replace(/[^\x20-\x7E]/gmi, "").replace("::", ":").trim();
                        copy = setHours(copy, map[data[i].Business].Hours.replace(/[^\x20-\x7E]/gmi, "").replace("::", ":").trim());

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

async function uploadDocuments(type) {
    var file = type == "add" ? "../output/service-nodes.json" : "../output/delete-services.json";
    var csd = new AWS.CloudSearchDomain({
        endpoint: DOCUMENT_DOMAIN
    });

    var documents = JSON.parse(fs.readFileSync(file, 'utf8'));
    var i, j, temparray, chunk = 1000, chunkcount = 0;

    for (i = 0, j = documents.length; i < j; i += chunk) {
        temparray = documents.slice(i, i + chunk);
        var params = {
            contentType: "application/json",
            documents: JSON.stringify(temparray)
        };

        try {
            await csd.uploadDocuments(params).promise();
            console.log("Succesfully uploaded chunk " + chunkcount);
        } catch (err) {
            console.log("There was a problem with chunk " + chunkcount);
            console.log(err);
        }

        chunkcount++;
    }
}

// This is a Cloudsearch hack to be able to filter by "open now"
function setHours(obj, str) {
    const daysArr = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const dayMap = {
        "Mon": "monday_hours",
        "Tue": "tuesday_hours",
        "Wed": "wednesday_hours",
        "Thu": "thursday_hours",
        "Fri": "friday_hours",
        "Sat": "saturday_hours",
        "Sun": "sunday_hours"
    };

    try {
        var dayIntervals = str.split(",");
        for (var i = 0; i < dayIntervals.length; i++) {
            var dayInterval = dayIntervals[i].trim();
            var daySubstring = dayInterval.substring(0, dayInterval.search(/\d/)).trim();
            var hoursSubstring = dayInterval.substring(dayInterval.search(/\d/)).trim();

            if (daySubstring.includes("-")) {
                var firstDay = daySubstring.split("-")[0].trim();
                var lastDay = daySubstring.split("-")[1].trim();

                firstDay = formatDay(firstDay);
                lastDay = formatDay(lastDay);

                for (var j = daysArr.indexOf(firstDay); j <= daysArr.indexOf(lastDay, daysArr.indexOf(firstDay)); j++) {
                    obj.fields[dayMap[daysArr[j]]] = translateHours(hoursSubstring);
                }
            } else {
                obj.fields[dayMap[daySubstring]] = translateHours(hoursSubstring);
            }
        }
    } catch (err) {
        console.log("There was a problem translating hours for string: " + str);
        console.log(err);
    }

    return obj;
}

function translateHours(hours) {
    var translated = [];
    var firstTime = hours.split("-")[0];
    var lastTime = hours.split("-")[1];

    firstTime = formatTime(firstTime);
    lastTime = formatTime(lastTime);

    var currentTime = firstTime;

    while (currentTime != lastTime) {
        if (currentTime == "24:00") {
            throw Error("Invalid time range: " + firstTime + "-" + lastTime);
        }
        translated.push(currentTime);
        currentTime = nextTime(currentTime);
    }

    return translated;
}

function formatTime(time) {
    if (time.substring(time.length - 2) != "am" && time.substring(time.length - 2) != "pm") {
        throw Error("There was a problem with time: " + time);
    }

    if (time.substring(time.length - 2) == "am") {
        time = time.substring(0, time.length - 2);
        if (time.substring(0, 2) == "12") {
            time = "00" + time.substring(2);
        }
    } else {
        time = time.substring(0, time.length - 2);
        if (time.substring(0, 2) != "12") {
            time = (parseInt(time.split(":")[0], 10) + 12).toString() + time.substring(time.indexOf(":"));
        }
    }

    if (time.substring(0, time.indexOf(":")).length == 1) {
        time = "0" + time;
    }

    return time;
}

function formatDay(day) {

    day = day.toUpperCase();

    if (day == "MO" || day == "MOND" || day == "MON") {
        return "Mon";
    }
    if (day == "TUES" || day == "TU" || day == "TUE") {
        return "Tue";
    }
    if (day == "WEND" || day == "WE" || day == "WEDN" || day == "WEDS" || day == "WED") {
        return "Wed";
    }
    if (day == "THUR" || day == "TH" || day == "THURS" || day == "THU") {
        return "Thu";
    }
    if (day == "FRID" || day == "FR" || day == "FRI") {
        return "Fri";
    }
    if (day == "SATU" || day == "SA" || day == "SAT") {
        return "Sat";
    }
    if (day == "SU" || day == "SUND" || day == "SUN") {
        return "Sun";
    }

    console.log("Failed to format day: " + day);
}

function nextTime(time) {
    if (time.substring(3) == "00") {
        time = time.substring(0, 3) + "15";
    } else if (time.substring(3) == "15") {
        time = time.substring(0, 3) + "30";
    } else if (time.substring(3) == "30") {
        time = time.substring(0, 3) + "45";
    } else if (time.substring(3) == "45") {
        time = (parseInt(time.substring(0, 2), 10) + 1).toString() + ":" + "00";
        if (time.substring(0, time.indexOf(":")).length == 1) {
            time = "0" + time;
        }
    } else {
        throw Error("Time minutes isn't a multiple of 15: " + time);
    }

    return time;
}


//deleteNodes();
//addNodes();
uploadDocuments("add");