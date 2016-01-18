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
var dateTrips = {};
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
      if (!calendar[record[1]]){
        calendar[record[1]] = {};
      }
      calendar[record[1]][record[0]] = true;

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
};

var populateRoutesIfNeeded = function(){
  var routesPopulated = "Done";
  if (Object.keys(routes).length === 0){
    routesPopulated = populateRoutes();
  }
  return routesPopulated;
};

var populateTripsIfNeeded = function(routeName, formattedDate, tomorrowFormattedDate){
  return function(){
    if (!routes[routeName.toUpperCase()]){
      throw Error("Badness");
    }
    route = routes[routeName.toUpperCase()];
    var areAllServiceIdsPresent = true;
    var checkServiceIdsPresentForDate = function(date){
      for (var serviceId in calendar[date]){
        if (!trips[serviceId]){
          areAllServiceIdsPresent = false;
          break;
        }

      }
    }
    checkServiceIdsPresentForDate(formattedDate);
    checkServiceIdsPresentForDate(tomorrowFormattedDate);
    if (!areAllServiceIdsPresent){
      return populateTrips(route.id);
    }
    return "Done";
  };
};

var populateStopsIfNeeded = function(firstStationName, secondStationName){
  return function(){
    if (!stops[firstStationName.toUpperCase()] || !stops[secondStationName.toUpperCase()]){
      return populateStops();
    }
    return "Done";
  };
};

var nextSchedules = module.exports.nextSchedules =
  function (numberOfSchedules, routeName, firstStationName, secondStationName){
    var nextSchedulesDeferment = Q.defer();
    var calendarPopulated = Q("Done");
    var now = new Date();
    var tomorrow = new Date(now.getTime() + 24*60*60*1000);
    var formattedDate = getYYYYMMDD(now);
    var tomorrowFormattedDate = getYYYYMMDD(tomorrow);
    var route, firstStationId, secondStationId;
    if (!calendar[formattedDate] || !calendar[tomorrowFormattedDate]){
      calendarPopulated = populateCalendar();
    }
    calendarPopulated.then(
        populateRoutesIfNeeded
      ).then(
        populateTripsIfNeeded(routeName, formattedDate, tomorrowFormattedDate)
      ).then(
        populateStopsIfNeeded(firstStationName, secondStationName)
      ).then(function(){
        firstStationId = stops[firstStationName.toUpperCase()].id;
        secondStationId = stops[secondStationName.toUpperCase()].id;
        var areAllGoodTripsPopulated = true;
        var checkServiceIdsPresentForDate = function(date){
          for (var serviceId in calendar[date]){
            if (!sortedGoodTrips[serviceId]){
              areAllGoodTripsPopulated = false;
              break;
            }
          }
        }
        checkServiceIdsPresentForDate(formattedDate);
        checkServiceIdsPresentForDate(tomorrowFormattedDate);
        if (!areAllGoodTripsPopulated){
          goodTrips = {};
          sortedGoodTrips = [];
          return populateStopTimes(firstStationId, secondStationId);
        }
        return "Done";
      }).then(function(){
        var currentTime = getTwoDigitValue(now.getHours()) + ":" +
          getTwoDigitValue(now.getMinutes()) + ":" +
          getTwoDigitValue(now.getSeconds());
        var tripSort =             function(a, b){
          if (a.data.departTime > b.data.departTime){
            return 1;
          }
          if (a.data.departTime < b.data.departTime){
            return -1;
          }
          if (a.data.departStation > b.data.departStation){
            return 1;
          }
          if (a.data.departStation < b.data.departStation){
            return -1;
          }
          return 0;
        }
        for (var serviceIdKey in goodTrips){
          sortedGoodTrips[serviceIdKey] = [];
          for (var tripId in goodTrips[serviceIdKey]){
            sortedGoodTrips[serviceIdKey].push({tripId: tripId, data: goodTrips[serviceIdKey][tripId]});
          }
          //this is just to work around bad sorts in gtfs.
          sortedGoodTrips[serviceIdKey].sort(tripSort);
        }
        var populateDateTripsIfNotPopulated = function(date){
          if (dateTrips[date]){
            return;
          }
          dateTrips[date] = [];
          var departInfoSet = {};
          for (var serviceId in calendar[date]){
            if (!sortedGoodTrips[serviceId]){
              //sometimes schedules are missing?
              continue;
            }
            for (var i = 0; i < sortedGoodTrips[serviceId].length; i++){
              var currentTrip = sortedGoodTrips[serviceId][i];
              var setKey = currentTrip.data.departTime + currentTrip.data.departStation + currentTrip.data.arriveTime + currentTrip.data.arriveStation;
              //watch out for duplicates
              if (departInfoSet[setKey]){
                break;
              }
              departInfoSet[setKey] = true;
              dateTrips[date].push(currentTrip);
            }
          }
          dateTrips[date].sort(tripSort);
        }
        populateDateTripsIfNotPopulated(formattedDate);
        populateDateTripsIfNotPopulated(tomorrowFormattedDate);

        var foundBits = [];
        var addToFoundBits = function(dateToCheck, isToday){
          for (var i = 0; i < dateTrips[dateToCheck].length; i++){
            var testedTrip = dateTrips[dateToCheck][i];
            if (!isToday || (testedTrip.data.departTime > currentTime || testedTrip.data.arriveTime > currentTime)){
              foundBits.push(testedTrip);
              if (foundBits.length === numberOfSchedules){
                break;
              }
            }
          }
        };
        addToFoundBits(formattedDate, true);
        if (foundBits.length !== numberOfSchedules){
          addToFoundBits(tomorrowFormattedDate);
        }
        nextSchedulesDeferment.resolve(foundBits);
        });
    return nextSchedulesDeferment.promise;
  }
;

//TODO: Account for service removal.
//TODO: Account for realtime GTFS.

var convert24To12 = function(time){
  var timeSplit = time.split(":");
  var postfix = " PM";
  timeSplit[0] = parseInt(timeSplit[0]);
  if (timeSplit[0] < 12 || timeSplit[0] > 23){
    postfix = " AM";
  }
  if (timeSplit[0] > 12){
    timeSplit[0] = timeSplit[0] - 12;
  }
  if (timeSplit[0] === 0){
    timeSplit[0] = 12;
  }
  return getTwoDigitValue(timeSplit[0]) + ":" +
    getTwoDigitValue(timeSplit[1]) + ":" +
    getTwoDigitValue(timeSplit[2]) + postfix;
};

nextSchedules(85, "Port Washington", "Great Neck", "Penn Station").then(function(result){
  for (var i = 0; i < result.length; i++){
    var goodTrip = result[i];
    console.log(stopIdReverseLookup[goodTrip.data.departStation]
      +
      ":" + convert24To12(goodTrip.data.departTime) +
      "  ->  " + stopIdReverseLookup[goodTrip.data.arriveStation] + ":" +
      convert24To12(goodTrip.data.arriveTime));
  }
}).fail(function(error){console.dir(error);});
