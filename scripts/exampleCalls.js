var AWS = require('aws-sdk');
AWS.config.loadFromPath('credentials.json');

const SEARCH_HELPER_DOMAIN = "search-helpers-rwq52jv2pag6js3cozrlqwxkry.us-east-1.cloudsearch.amazonaws.com";
const SERVICE_DOMAIN = "search-services-dvsyeourhhah4hhenkpm2baqoa.us-east-1.cloudsearch.amazonaws.com";
const EXAMPLE_USER_INPUT = 'man ped';
const EXAMPLE_SERVICE_SEARCH = 'nails';

function sampleSearchHelpers() {
  var searchHelpers = new AWS.CloudSearchDomain({
    endpoint: SEARCH_HELPER_DOMAIN
  });

  var params = {
    query: EXAMPLE_USER_INPUT, // put the user typed text here
    suggester: 'suggestor',
    size: 10
  };
  
  searchHelpers.suggest(params, function (err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else console.log(JSON.stringify(data)); // successful response - These are your search helpers
  });
}

function sampleServiceSearch() {
  var serviceSearch = new AWS.CloudSearchDomain({
    endpoint: SERVICE_DOMAIN
  });

  var params = {
    query: EXAMPLE_SERVICE_SEARCH, // put the selected search helper/ search text here,
    expr:
    `
    {
      "rank": "_score/log10(haversin(37.7763486,-122.4179174,geolocation.latitude,geolocation.longitude))"
    }
    `,
    sort: 'rank asc'
  };
  
  serviceSearch.search(params, function (err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else console.log(JSON.stringify(data)); // successful response - These are your search helpers
  });
}

sampleServiceSearch();