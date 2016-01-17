var Q = require("q")
  , fs = require("fs")
  , request = require('request')
  , unzip = require('unzip')
  , parse = require('csv-parse')

  ;
var routes = {};
var trips = {};
var goodTrips = {};
var sortedGoodTrips = [];
var calendar = {};
var stops = {};
var lastServiceId = '';
var fetchGTFSFromMTA = function(){
  var deferment = Q.defer();
  if (!fs.existsSync('./gtfsdata')){
    fs.mkdirSync('./gtfsdata');
  }
  request('http://web.mta.info/developers/data/lirr/google_transit.zip')
    .pipe(unzip.Extract({path:'./gtfsdata'}))
    .on('close', function(){
      deferment.resolve("Done");
    });
  return deferment.promise;
};

var populateObjectFromFile = function(filepath, recordFunction){
  var deferment = Q.defer();
  var stream =fs.createReadStream(filepath);
  parser = parse();
  var recordCount = 0;
  parser.on('readable', function(){
    while(record = parser.read()){
      if (recordCount != 0){
        recordFunction(record);
      }
      recordCount++;
    }
  });
  parser.on('finish', function(){
    deferment.resolve('done');
  });
  stream.pipe(parser);
  return deferment.promise;
};

var populateCalendar = function(){
  var gtfsPromise = Q("Done");
  if (!fs.existsSync('./gtfsdata/calendar_dates.txt')){
    gtfsPromise = fetchGTFSFromMTA();
  }

  return gtfsPromise.then(function(){
    return populateObjectFromFile('./gtfsdata/calendar_dates.txt', function(record){
      calendar[record[1]] = record[0];
    });
  });
};

var populateRoutes = function(){
  return populateObjectFromFile('./gtfsdata/routes.txt', function(record){
    routes[record[2].toUpperCase()] = {
          id:record[0],
          friendlyName: record[2]
        };
      });
};

var populateTrips = function(routeId, serviceId){
  return populateObjectFromFile('./gtfsdata/trips.txt', function(record){
    if (record[0] === routeId && record[1] == serviceId){
      trips[record[2]] = {direction: record[5]};
    }
  });
};

var populateStops = function(){
  return populateObjectFromFile('./gtfsdata/stops.txt', function(record){
    stops[record[1].toUpperCase()] = {id: record[0], friendlyName: record[1]};
  });
};

var isFirstValueEarlier = function(firstValue, secondValue){
  return firstValue < secondValue;
};

var populateStopTimes = function(firstStationId, secondStationId){
  tripsChecker = {};
  return populateObjectFromFile('./gtfsdata/stop_times.txt', function(record){
    if (trips[record[0]]){
      if (!tripsChecker[record[0]]){
        tripsChecker[record[0]] = {};
      }
      if (record[3] === firstStationId){
        tripsChecker[record[0]][firstStationId] = record[1];
        if (tripsChecker[record[0]][secondStationId]){
          goodTrips[record[0]] = {};
        }
      }
      if (record[3] === secondStationId){
        tripsChecker[record[0]][secondStationId] = record[1];
        if (tripsChecker[record[0]][firstStationId]){
          goodTrips[record[0]] = {};
        }
      }
      if (goodTrips[record[0]]){
        goodTrips[record[0]] = tripsChecker[record[0]];
        if (isFirstValueEarlier(
          goodTrips[record[0]][firstStationId],
          goodTrips[record[0]][secondStationId])){
          goodTrips[record[0]].departStation = firstStationId;
          goodTrips[record[0]].departTime = goodTrips[record[0]][firstStationId];
          goodTrips[record[0]].arriveStation = secondStationId;
          goodTrips[record[0]].arriveTime = goodTrips[record[0]][secondStationId];
        }else{
          goodTrips[record[0]].departStation = secondStationId;
          goodTrips[record[0]].departTime = goodTrips[record[0]][secondStationId];
          goodTrips[record[0]].arriveStation = firstStationId;
          goodTrips[record[0]].arriveTime = goodTrips[record[0]][firstStationId];
        }
      }
    }
  });
};

var getTwoDigitValue = function(input){
  if (("" + input).length == 1){
    return "0" + input;
  }
  return "" + input;
};


var nextSchedules = module.exports.nextSchedules =
  function (numberOfSchedules, routeName, firstStationName, secondStationName){
    var calendarPopulated = Q("Done");
    var now = new Date();
    var formattedDate = "" + now.getFullYear() + getTwoDigitValue(now.getMonth() + 1) + getTwoDigitValue(now.getDate());
    var route, serviceId, firstStationId, secondStationId;
    var stopIdReverseLookup = {};
    if (!calendar[formattedDate]){
      calendarPopulated = populateCalendar();
    }
    calendarPopulated.then(function(){
      serviceId = calendar[formattedDate];
      var routesPopulated = Q("Done");
      if (Object.keys(routes).length === 0){
        routesPopulated = populateRoutes();
      }
      return routesPopulated;
    }).then(function(){
      if (!routes[routeName.toUpperCase()]){
        throw Error("Badness");
      }
      route = routes[routeName.toUpperCase()];
      if (serviceId !== lastServiceId){
        trips = {};
        return populateTrips(route.id, serviceId);
      }
      return Q("Done");
    }).then(function(){
      if (!stops[firstStationName.toUpperCase()] || !stops[secondStationName.toUpperCase()]){
        return populateStops();
      }
        return "Done";

      }).then(function(){
        firstStationId = stops[firstStationName.toUpperCase()].id;
        secondStationId = stops[secondStationName.toUpperCase()].id;
        stopIdReverseLookup[firstStationId] = firstStationName;
        stopIdReverseLookup[secondStationId] = secondStationName;
        if (serviceId !== lastServiceId){
          goodTrips = {};
          sortedGoodTrips = [];
          return populateStopTimes(firstStationId, secondStationId);
        }
        return "Done";
      }).then(function(){
        lastServiceId = serviceId;
        var currentTime = getTwoDigitValue(now.getHours()) + ":" +
          getTwoDigitValue(now.getMinutes()) + ":" +
          getTwoDigitValue(now.getSeconds());
        for (var tripId in goodTrips){
          sortedGoodTrips.push({tripId: tripId, data: goodTrips[tripId]});
        }
        //this is just to work around bad sorts in gtfs.
        sortedGoodTrips.sort(
          function(a, b){
            if (a.data.departTime > b.data.departTime){
              return 1;
            }
            if (a.data.departTime < b.data.departTime){
              return -1;
            }
            return 0;
          });
        var foundBits = 0;
        for (var i = 0; i < sortedGoodTrips.length; i++){
          var testedTrip = sortedGoodTrips[i];
          if (testedTrip.data.departTime > currentTime || testedTrip.data.arriveTime > currentTime){
            foundBits++;
            console.log(stopIdReverseLookup[testedTrip.data.departStation]
              +
              ":" + testedTrip.data.departTime +
            "  ->  " + stopIdReverseLookup[testedTrip.data.arriveStation] + ":" + testedTrip.data.arriveTime);
            if (foundBits === numberOfSchedules){
              break;
            }
          }
        }
        });

  }
;

//TODO: Account for not finding enough trips in current day. Will require restructuring.
//TODO: Account for realtime GTFS.
//TODO: Determine how best to make 12H display.

nextSchedules(5, "Port Washington", "Great Neck", "Penn Station");