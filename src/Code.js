/**
Configuration Block
**/
function getConfig(request) {
  var config = {
    configParams: [
      {
        type: "INFO",
        name: "connect",
        text: "PLEASE NOTE: This connector requires and domain and dataset ID (found at the end of the target dataset URL) to get started. Please refer to our support documentation on locating the identifier for assistance: https://goo.gl/7CZBtf"
      },
      {
        type: 'TEXTINPUT',
        name: 'domain',
        displayName: 'Domain',
        helpText: 'Copy and paste the Domain (e.g. https://opendata.socrata.com).',
        placeholder: 'https://opendata.socrata.com'
      },
      {
        type: 'TEXTINPUT',
        name: 'id',
        displayName: 'Dataset ID',
        helpText: 'Copy and paste the dataset ID (e.g. 1234-abcd) from the end of the target dataset URL',
        placeholder: '4jxs-y9s7'
      },
      {
        type: "INFO",
        name: "userandpass",
        text: "Username and Password are required to access private datasets"
      },
      {
        type: 'TEXTINPUT',
        name: 'username',
        displayName: '(optional) Username: ',
        helpText: 'Your Socrata Username',
        placeholder: ''
       },
       {
         type: 'TEXTINPUT',
         name: 'password',
         displayName: '(optional) Password: ',
         helpText: 'Your Socrata Password',
         placeholder: ''
       }
    ]
  };
  return config;
};
/**
Authorization Block
**/
function getAuthType() {
  var response = {
    "type": "NONE"
  };
  return response;
}
/**
Dataset Field Mapping
**/
function toField(tableSchemaField) {
    switch (tableSchemaField.dataTypeName) {
        case 'boolean':
            ftype = 'BOOLEAN';
            format = 'text';
            break;
        case 'number':
        case 'integer':
            ftype = 'NUMBER';
            break;
        case 'money' :
            ftype = 'CURRENCY';
            break;
        default:
            ftype = 'STRING';
    }

    return {
        'name': tableSchemaField.fieldName,
        'label': tableSchemaField.name,
        'dataType': ftype
    }
}
function toTableSchema(schemaRow) {
    return schemaRow.map(toField);
}
/**
Schema Initialization
**/
function schemaInit(domain, ID, USERNAME, PASSWORD) {
  id_regex = RegExp('[a-z0-9]{4}-[a-z0-9]{4}');
  if(!id_regex.test(ID)) {
    throw new Error("DS_USER:Invalid Dataset ID, must be in form abcd-1234");
  }
  if(USERNAME && PASSWORD) {
    var params = {
      method: 'get',
      headers: {
        Authorization: "Basic "+ Utilities.base64Encode(USERNAME + ':' + PASSWORD),
        Accept: "application/json"
      },
      escaping: false
    };
  } else {
    var params = { method: 'get', escaping: false };
  }

  var c = [domain, "/api/views/", ID, "/columns.json"];
  var url = c.join("");
  var response = UrlFetchApp.fetch(url, params);
  var schema = JSON.parse(response);
  return schema;
}
function getSchema(request) {
    Logger.log("Getting Schema");
    var domain = request.configParams.domain;
    var datasetID = request.configParams.id;
    var username = request.configParams.username;
    var password = request.configParams.password;
    var tableSchema = toTableSchema(schemaInit(domain, datasetID, username, password));
    return {'schema': tableSchema};
}

/**
Data Initializations
**/
function dataInit(domain, ID, api_fields, USERNAME, PASSWORD) {
  if(USERNAME || PASSWORD) {
    var params = {
      method: 'get',
      headers: {
        Authorization: "Basic "+ Utilities.base64Encode(USERNAME + ':' + PASSWORD),
        Accept: "application/json"
      },
      escaping: false
    };
  } else {
    var params = { method: 'get', escaping: false }
  }
  // Fetch total number of rows
  query = "$select=count(*) as count";
  var c = [domain, "/resource/", ID, ".json?", query];

  var url = encodeURI(c.join(""));

  JSON.parse(UrlFetchApp.fetch(url, params))
  var count = parseInt(JSON.parse(UrlFetchApp.fetch(url, params))[0]["count"]);

  // Iterate through and collect data
  // at 50,000 rows per call
  data = [];
  for(var i = 0; i < count; i += 50000) {
    if(i === 0) {
      c = [domain, "/resource/", ID, ".json?$limit=50000&$select=", api_fields.join(",")];
    } else {
      c = [domain, "/resource/", ID, ".json?$limit=50000&$offset=", i.toString(), "&$select=", api_fields.join(",")];
    }
    url = encodeURI(c.join(""));
    var j = UrlFetchApp.fetch(url, params);
    var d = JSON.parse(UrlFetchApp.fetch(url, params));
    data.push.apply(data, d);
  }
  return data;
}

function toRowResponse(fieldNames, row) {
    return {
        'values': fieldNames.map(function (field) {
          if(field.dataType === "number") {
            return parseInt(row[field.name]);
          } else {
            return row[field.name];
          }
        })
    };
}

function getData(request) {
  var domain = request.configParams.domain;
  var datasetID = request.configParams.id;
  var username = request.configParams.username;
  var password = request.configParams.password;

  var dataSchema = [];
  var socrataSchema = schemaInit(domain, datasetID, username, password);
  var tableSchema = toTableSchema(socrataSchema);
  var api_fields = [];
  var formatted_fields = [];
  var api_query = [];
  var unmappedData = [];
  for(var i = 0; i < tableSchema.length; i++) {
    request.fields.forEach(function(user) {
        if (user.name === tableSchema[i].name) {
            dataSchema.push(tableSchema[i]);
            api_fields.push(user.name);
          if(tableSchema[i].dataType === "number") {
            formatted_fields.push({"name":user.name, "format":"number"});
          }
          if(socrataSchema[i].dataTypeName === "calendar_date") {
            formatted_fields.push({"name":user.name, "format":"date"});
          }
        }
    });
    unmappedData = dataInit(domain, datasetID, api_fields, username, password);
  }

  var data = unmappedData.map(function(row) {
    return toRowResponse(dataSchema, row);
  });
  return {
    schema: dataSchema,
    rows: data
  };
};
/**
Test function
**/
function test() {
  var testDomain = "https://metropolis.demo.socrata.com";
  var testID = "h4ws-ns8a";

  var user_fields = [{"name":"incident_id","label":"Employee Status","dataType":"STRING"},{"name":"division","label":"Incident ID","dataType":"STRING"}];
  var api_fields = ["incident_id", "division"];
  //var api_queries = [testDateCol, monthsAgo.format("YYYY-MM-DD"), upto.format("YYYY-MM-DD")];

  var schema = schemaInit(testDomain, testID);
  //var tableSchema = toTableSchema(schema);
  //var dataDyn = dataDynamicInit(testDomain, testID, api_fields, api_queries);
  //var mapped = dataDyn.map(function(row) { return toRowResponse(tableSchema, row); });
  //var data = dataInit(testDomain, testID, api_fields);
  //var mapped = data.map(function(row) { return toRowResponse(tableSchema, row); });
  //Logger.log(mapped.length);
}
