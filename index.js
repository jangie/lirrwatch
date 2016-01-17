var Q = require("q")
  , fs = require("fs")
  , request = require('request')
  , unzip = require('unzip')
  , parse = require('csv-parse')

  ;
var routes = {};
var trips = {};
var goodTrips = {};
var sortedGoodTrips = {};
var calendar = {};
var stops = {};
var stopIdReverseLookup = {};

var fetchGTFSFromMTA = function(){
  //cleanup
  routes = {};
  trips = {};
  goodTrips = {};
  sortedGoodTrips = {};
  calendar = {};
  stops = {};
  stopIdReverseLookup = {};
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

var populateTrips = function(routeId){
  return populateObjectFromFile('./gtfsdata/trips.txt', function(record){
    if (!trips[record[1]]){
      trips[record[1]] = {};
    }
    if (record[0] === routeId ){
      trips[record[1]][record[2]] = {direction: record[5]};
    }
  });
};

var populateStops = function(){
  return populateObjectFromFile('./gtfsdata/stops.txt', function(record){
    stops[record[1].toUpperCase()] = {id: record[0], friendlyName: record[1]};
    stopIdReverseLookup[record[0]] = record[1];
  });
};

var isFirstValueEarlier = function(firstValue, secondValue){
  return firstValue < secondValue;
};

var populateStopTimes = function(firstStationId, secondStationId){
  tripsChecker = {};
  var tripIds = {};
  for (var serviceId in trips){
    for (var tripId in trips[serviceId]){
      tripIds[tripId] = serviceId;
    }
  }
  return populateObjectFromFile('./gtfsdata/stop_times.txt', function(record){
    if (tripIds[record[0]]){
      var stopServiceId = tripIds[record[0]];
      if (!tripsChecker[stopServiceId]){
        tripsChecker[stopServiceId] = {};
      }
      if (!tripsChecker[stopServiceId][record[0]]){
        tripsChecker[stopServiceId][record[0]] = {};
      }
      if (!goodTrips[stopServiceId]){
        goodTrips[stopServiceId] = {};
      }
      if (record[3] === firstStationId){
        tripsChecker[stopServiceId][record[0]][firstStationId] = record[1];
        if (tripsChecker[stopServiceId][record[0]][secondStationId]){
          goodTrips[stopServiceId][record[0]] = {};
        }
      }
      if (record[3] === secondStationId){
        tripsChecker[stopServiceId][record[0]][secondStationId] = record[1];
        if (tripsChecker[stopServiceId][record[0]][firstStationId]){
          goodTrips[stopServiceId][record[0]] = {};
        }
      }
      if (goodTrips[stopServiceId][record[0]]){
        goodTrips[stopServiceId][record[0]] = tripsChecker[stopServiceId][record[0]];
        if (isFirstValueEarlier(
          goodTrips[stopServiceId][record[0]][firstStationId],
          goodTrips[stopServiceId][record[0]][secondStationId])){
          goodTrips[stopServiceId][record[0]].departStation = firstStationId;
          goodTrips[stopServiceId][record[0]].departTime = goodTrips[stopServiceId][record[0]][firstStationId];
          goodTrips[stopServiceId][record[0]].arriveStation = secondStationId;
          goodTrips[stopServiceId][record[0]].arriveTime = goodTrips[stopServiceId][record[0]][secondStationId];
        }else{
          goodTrips[stopServiceId][record[0]].departStation = secondStationId;
          goodTrips[stopServiceId][record[0]].departTime = goodTrips[stopServiceId][record[0]][secondStationId];
          goodTrips[stopServiceId][record[0]].arriveStation = firstStationId;
          goodTrips[stopServiceId][record[0]].arriveTime = goodTrips[stopServiceId][record[0]][firstStationId];
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

var getYYYYMMDD = function(desiredDateObject){
  return "" + desiredDateObject.getFullYear() +
    getTwoDigitValue(desiredDateObject.getMonth() + 1) +
    getTwoDigitValue(desiredDateObject.getDate())
}

var nextSchedules = module.exports.nextSchedules =
  function (numberOfSchedules, routeName, firstStationName, secondStationName){
    var nextSchedulesDeferment = Q.defer();
    var calendarPopulated = Q("Done");
    var now = new Date();
    var tomorrow = new Date(now.getTime + 24*60*60*1000);
    var formattedDate = getYYYYMMDD(now);
    var tomorrowFormattedDate = getYYYYMMDD(tomorrow);
    var route, serviceId, tomorrowServiceId, firstStationId, secondStationId;
    if (!calendar[formattedDate] || !calendar[tomorrowFormattedDate]){
      calendarPopulated = populateCalendar();
    }
    calendarPopulated.then(function(){
      serviceId = calendar[formattedDate];
      tomorrowServiceId = calendar[tomorrowFormattedDate];
      var routesPopulated = "Done";
      if (Object.keys(routes).length === 0){
        routesPopulated = populateRoutes();
      }
      return routesPopulated;
    }).then(function(){
      if (!routes[routeName.toUpperCase()]){
        throw Error("Badness");
      }
      route = routes[routeName.toUpperCase()];
      if (!trips[serviceId] || !trips[tomorrowServiceId]){
        return populateTrips(route.id);
      }
      return "Done";
    }).then(function(){
      if (!stops[firstStationName.toUpperCase()] || !stops[secondStationName.toUpperCase()]){
        return populateStops();
      }
        return "Done";

      }).then(function(){
        firstStationId = stops[firstStationName.toUpperCase()].id;
        secondStationId = stops[secondStationName.toUpperCase()].id;
        if (!sortedGoodTrips[serviceId] || !sortedGoodTrips[tomorrowServiceId]){
          goodTrips = {};
          sortedGoodTrips = [];
          return populateStopTimes(firstStationId, secondStationId);
        }
        return "Done";
      }).then(function(){
        var currentTime = getTwoDigitValue(now.getHours()) + ":" +
          getTwoDigitValue(now.getMinutes()) + ":" +
          getTwoDigitValue(now.getSeconds());
        if (!sortedGoodTrips[serviceId]){
          for (var serviceIdKey in goodTrips){
            sortedGoodTrips[serviceIdKey] = [];
            for (var tripId in goodTrips[serviceIdKey]){
              sortedGoodTrips[serviceIdKey].push({tripId: tripId, data: goodTrips[serviceIdKey][tripId]});
            }
            //this is just to work around bad sorts in gtfs.
            sortedGoodTrips[serviceIdKey].sort(
              function(a, b){
                if (a.data.departTime > b.data.departTime){
                  return 1;
                }
                if (a.data.departTime < b.data.departTime){
                  return -1;
                }
                return 0;
              });
          }
        }
        var foundBits = [];
        var addToFoundBits = function(serviceIdToCheck){
          for (var i = 0; i < sortedGoodTrips[serviceIdToCheck].length; i++){
            var testedTrip = sortedGoodTrips[serviceIdToCheck][i];
            if (testedTrip.data.departTime > currentTime || testedTrip.data.arriveTime > currentTime){
              foundBits.push(testedTrip);
              if (foundBits.length === numberOfSchedules){
                break;
              }
            }
          }
        }
        addToFoundBits(serviceId);
        if (!foundBits.length === numberOfSchedules){
          addToFoundBits(tomorrowServiceId);
        }
        nextSchedulesDeferment.resolve(foundBits);
        });
    return nextSchedulesDeferment.promise;
  }
;

//TODO: Account for realtime GTFS.
//TODO: Determine how best to make 12H display.

nextSchedules(5, "Port Washington", "Great Neck", "Penn Station").then(function(result){
  for (var i = 0; i < result.length; i++){
    var goodTrip = result[i];
    console.log(stopIdReverseLookup[goodTrip.data.departStation]
      +
      ":" + goodTrip.data.departTime +
      "  ->  " + stopIdReverseLookup[goodTrip.data.arriveStation] + ":" + goodTrip.data.arriveTime);
  }

});
