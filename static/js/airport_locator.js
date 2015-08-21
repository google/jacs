var map;
var airports = [];
var place;
var previousFeatures;
var attempts = 0;
var radius;
var featureId = 0;
var authorized = false;
var newAirportMarker;
var fields = [
    'OGR_FID',
    'abbrev',
    'featurecla',
    'gps_code',
    'iata_code',
    'location',
    'name',
    'natlscale',
    'scalerank',
    'type',
    'wikipedia',
];

function initialize() {
  var mapOptions = {
    center: new google.maps.LatLng(42, -99),
    zoom: 5
  };
  map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);

  var input = (document.getElementById('location-input'));
  map.controls[google.maps.ControlPosition.TOP_LEFT].push(input);

  var authorize = (document.getElementById('authorize-button'));
  map.controls[google.maps.ControlPosition.TOP_RIGHT].push(authorize);

  authorizationFlow(authorizationComplete, refreshComplete);

  var autocomplete = new google.maps.places.Autocomplete(input);
  autocomplete.bindTo('bounds', map);

  google.maps.event.addListener(autocomplete, 'place_changed', function() {
    place = autocomplete.getPlace();
    if (!place.geometry) {
      return;
    }

    map.setCenter(place.geometry.location);
    map.setZoom(8);

    // reset variables
    airports = [];
    radius = 25000;
    attempts = 0;
    var url = createAirportsRequest();
    sendPostRequest(url);
  });

  google.maps.event.addListener(map, 'click', function(event) {
    // show form to inster a new airport
    var airportDiv = document.getElementById('insert-airport');
    airportDiv.style.display = 'block';
    var nameInput = document.getElementById('name-input');
    nameInput.style.display = 'none';
    var editButton = document.getElementById('edit-button');
    editButton.style.display = 'none';
    var deleteButton =  document.getElementById('delete-button');
    deleteButton.style.display = 'none';

    // remove the previous marker if any
    if (newAirportMarker !== undefined) {
      newAirportMarker.setMap(null);
    }
    newAirportMarker = new google.maps.Marker({
      position: event.latLng,
      map: map,
      title: 'New Airport'
    });
  });
}

function createAirportsRequest() {
  var tableId = 'airports';
  var lng = place.geometry.location.lng();
  var lat = place.geometry.location.lat();
  var url = '/tables/' + tableId
      + '/features?'
      + '&intersects=CIRCLE(' + lng + ' ' + lat + ', ' + radius + ')'
      + "&where=type NOT LIKE '%military%'"
      + '&limit=15';

  return url;
}

function sendPostRequest(url) {
  var xmlHttp = new XMLHttpRequest();
  xmlHttp.open("GET", url, true);
  xmlHttp.onload = function (e) {
    if (xmlHttp.readyState === 4) {
      if (xmlHttp.status === 200) {
        processResponse(JSON.parse(xmlHttp.responseText));
      } else {
        console.error(xmlHttp.statusText);
        // retry the query, up to 3 times
        if (attempts < 3) {
          sendPostRequest(url);
          attempts++;
        }
      }
    }
  };
  xmlHttp.onerror = function (e) {
    console.error(xmlHttp.statusText);
  };
  xmlHttp.send(null);
}

function processResponse(response) {
  if (response.features.length < 5) {
    // increase radius
    radius = radius + radius;
    sendPostRequest(createAirportsRequest());
  }
  else {
    airports = response;
    displayResults();
  }
}

function displayResults() {
  // let's remove the previous features
  if (previousFeatures !== undefined) {
    previousFeatures.forEach(function(feature) {
      map.data.remove(feature);
    });
  }

  // now let's display the new ones!
  console.log('airports returned:', airports);
  previousFeatures = map.data.addGeoJson(airports);
  console.log('airports returned:', previousFeatures);

  map.data.addListener('click', function(event) {
    if (authorized) {
      featureId = event.feature.getProperty('OGR_FID');

      var deleteButton = document.getElementById('delete-button');
      deleteButton.style.display = 'block';

      var editButton = document.getElementById('edit-button');
      editButton.style.display = 'block';

      var nameInput = document.getElementById('name-input');
      nameInput.value = event.feature.getProperty('name');
      nameInput.style.display = 'block';

      map.controls[google.maps.ControlPosition.TOP_RIGHT].clear();
      map.controls[google.maps.ControlPosition.TOP_RIGHT].push(deleteButton);
      map.controls[google.maps.ControlPosition.TOP_RIGHT].push(editButton);
      map.controls[google.maps.ControlPosition.TOP_RIGHT].push(nameInput);

      var airportDiv = document.getElementById('insert-airport');
      airportDiv.style.display = 'none';
    }
    else {
      alert('You first need to authorize this application in order to edit data');
    }
  });
}

function authorizeEditName() {
  console.log('authorize Edit Name', editName);
  authorizationFlow(editName, refreshComplete);
}

function editName(user) {
  var nameInput = document.getElementById('name-input');
  var url = "/tables/airports/features/batchPatch";
  var data = {
    'features': [
      {
        'properties': {
          'OGR_FID': Number(featureId),
          'name': nameInput.value
        }
      }
    ]
  };
  console.log('batchPatch', user, nameInput, url, data);

  jQuery.ajax({
    type: 'PATCH',
    url: url,
    contentType: 'application/json',
    data: JSON.stringify(data),
    success: function() {
      // refresh data to reflect the changes
      airports = [];
      // radius = 5000; use the previous radius
      attempts = 0;
      var url = createAirportsRequest();
      sendPostRequest(url);
      alert('Value updated correctly!');
    },
    error: function(response) {
      console.log("Error: ", response);
    }
  });
}

function authorizeDeleteFeature() {
  authorizationFlow(deleteFeature, refreshComplete);
}

function deleteFeature(authResult) {
  var url = "/tables/airports/features/batchDelete";
  var data = {
    'primary_keys': [
      Number(featureId),
    ]
  };

  jQuery.ajax({
    type: 'POST',
    url: url,
    contentType: 'application/json',
    data: JSON.stringify(data),
    success: function() {
      // refresh data to reflect the changes
      airports = [];
      radius = 5000;
      attempts = 0;
      var url = createAirportsRequest();
      sendPostRequest(url);
      alert('Airport removed correctly!');
    },
    error: function(response) {
      response = JSON.parse(response.responseText);
      console.log("Error: ", response);
    }
  });
}

function authorizeCreateFeature() {
  if (validateForm() == true) {
    authorizationFlow(createFeature, refreshComplete);
  }
}

function validateForm() {
  var id = document.getElementById('new-id-input').value;
  var name = document.getElementById('new-name-input').value;
  var elevation = document.getElementById('new-elevation_ft-input').value;
  if (id == null || id == "") {
    alert("Please enter a (numeric) ID for the airport");
    return false;
  }
  if (name == null || name == "") {
    alert("Please enter a name for the airport");
    return false;
  }
  if (elevation == null || elevation == "") {
    alert("Please enter the elevation of the airport");
    return false;
  }
  return true;
}

function createFeature(authResult) {
  var url = "/tables/airports/features/batchInsert";

  for (var i = 0; i < fields.length; i++) {
    document.getElementById('new-' + fields[i] + '-input').value;
  }

  var data = {
    'features': [
      {
        'type': 'Feature',
        'geometry': {
          'type': 'Point',
          'coordinates': [
            newAirportMarker.getPosition().lng(),
            newAirportMarker.getPosition().lat()
          ]
        },
        'properties': {
	    'OGR_FID': parseInt(document.getElementById('new-id-input').value),
	    'abbrev': document.getElementById('new-ident-input').value,
	    'featurecla': 'Airport',
	    'gps_code': document.getElementById('new-gps_code-input').value,
	    'iata_code': document.getElementById('new-iata_code-input').value,
	    'location': document.getElementById('new-location-input').value,
	    'name': document.getElementById('new-name-input').value,
	    'natlscale': parseInt(document.getElementById('new-natlscale-input').value),
	    'scalerank': parseInt(document.getElementById('new-scalerank-input').value),
	    'type': document.getElementById('new-type-input').value,
	    'wikipedia': document.getElementById('new-wikipedia_link-input').value,
        }
      }
    ]
  };

  jQuery.ajax({
    type: 'POST',
    url: url,
    contentType: 'application/json',
    data: JSON.stringify(data),
    success: function() {
      var airportDiv = document.getElementById('insert-airport');
      airportDiv.style.display = 'none';
      newAirportMarker.setMap(null);
      // refresh data to reflect the changes
      airports = [];
      radius = 5000;
      attempts = 0;
      var url = createAirportsRequest();
      sendPostRequest(url);
      alert('Airport added correctly!');
    },
    error: function(response) {
      response = JSON.parse(response.responseText);
      console.log("Error: ", response);
    }
  });
}

function authorizationComplete(user) {
  // Check that the request works
  var url = "/tables/airports/features?limit=1";

  jQuery.ajax({
    url: url,
    dataType: 'json',
    success: function(response) {
      // Log the details of the Map.
      console.log(response);
    },
    error: function(response) {
      console.log("Error: ", response);
    }
  });
}

function refreshComplete() {
  // The refreshed token is automatically stored and used by gapi.client for
  // any additional requests, so we do not need to do anything in this handler.
}

// The entry point to the auth flow.
// authorizationComplete is called with an access_token when the oauth flow
// first completes.
// refreshComplete is called with a new access_token once the token refreshes.
function authorizationFlow(authorization_complete, refresh_complete) {
  var authorizeButton = document.getElementById('authorize-button');
  checkAuth(false, handleAuthResult);

  function checkAuth(prompt_user, callback) {
    console.log('checkAuth(' + prompt_user+', ...)');
    var xmlHttp = new XMLHttpRequest();
    console.log('GET /api/user/me?url='+window.location.href);
    console.log('xmlHttp.readystate = ' + xmlHttp.readystate);
    xmlHttp.open("GET", '/api/user/me?url='+window.location.href, true);
    xmlHttp.onload = function (e) {
      console.log('xmlHttp.readyState = ' + xmlHttp.readyState);
      if (xmlHttp.readyState === 4) {
	user = JSON.parse(xmlHttp.responseText);
	if (prompt_user && xmlHttp.status === 401) {
	  window.location.href = user.url;
	}
	callback(user);
      }
    };
    xmlHttp.send();
  }

  function handleAuthResult(user) {
    console.log('handleAuthResult', user);
      // Has the user authorized this application?
      if (user && user.admin) {
	// The application is authorized. Hide the 'Authorization' button.
	if (authorizeButton) {
	  authorizeButton.style.display = 'none';
	}
	authorization_complete(user);
	authorized = true;
      } else {
	// The application has not been authorized. Start the authorization flow
	// when the user clicks the button.
	if (authorizeButton) {
	  authorizeButton.style.display = 'block';
	}
	authorizeButton.onclick = handleAuthClick;
      }
  }

  function handleAuthClick(event) {
    checkAuth(true, handleAuthResult);
    return false;
  }

  function refreshToken() {
    checkAuth(false, refreshComplete);
  }

  function refreshComplete(user) {
    if (user && user.admin) {
      console.log('fake refresh complete', user);
      refresh_complete(user);
    }
    else {
      authorizeButton.style.display = 'block';
      authorizeButton.onclick = handleAuthClick;
    }
  }
}
