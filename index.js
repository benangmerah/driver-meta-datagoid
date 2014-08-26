var util = require('util');

var async = require('async');
var ckan = require('ckan');
var request = require('request');

var BmDriverBase = require('benangmerah-driver-base');

var RDF_NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
var RDFS_NS = 'http://www.w3.org/2000/01/rdf-schema#';
var OWL_NS = 'http://www.w3.org/2002/07/owl#';
var XSD_NS = 'http://www.w3.org/2001/XMLSchema#';
var BM_NS = 'http://benangmerah.net/ontology/';

function DataGoIdMetaDriver() {
  this.added = {};
}
util.inherits(DataGoIdMetaDriver, BmDriverBase);

module.exports = DataGoIdMetaDriver;

DataGoIdMetaDriver.prototype.setOptions = function(options) {
  if (!options) {
    options = {};
  }

  this.options = {
    ckanURL: options.ckanURL || 'http://data.ukp.go.id/',
    concurrency: options.concurrency || 50,
    limit: options.limit || 1000,
    timeout: options.timeout || 30000,
    graphBase: options.graphBase || 'tag:pdi:',
    titlePrefix: options.titlePrefix || 'Data.go.id: ',
    driverName: options.driverName || 'benangmerah-driver-datagoid'
  };
};

DataGoIdMetaDriver.prototype.fetch = function() {
  var self = this;

  self.client = new ckan.Client(self.options.ckanURL);

  self.listDatasets(function(err) {
    if (err) {
      return self.error(err);
    }

    self.finish();
  });
};

DataGoIdMetaDriver.prototype.listDatasets = function(callback) {
  var self = this;
  self.searchDatasets('kode_provinsi', function(err) {
    if (err) {
      return callback(err);
    }

    self.searchDatasets('kode_kabkota', callback);
  });
};

DataGoIdMetaDriver.prototype.searchDatasets = function(q, callback) {
  var self = this;

  self.client.action(
    'package_search',
    { q: q, rows: self.options.limit },
    function(err, data) {
      if (err) {
        return self.error(err);
      }

      self.info('Found ' + data.result.count + ' probable datasets. ' +
                'Fetching ' + data.result.results.length + '.');

      async.eachLimit(
        data.result.results,
        self.options.concurrency,
        self.processDataset.bind(self),
        callback
      );
    });
};

DataGoIdMetaDriver.prototype.processDataset = function(dataset, callback) {
  var self = this;

  if (self.added[dataset.name]) {
    return callback();
  }

  var resource = dataset.resources[0];
  if (!resource || resource.format !== 'CSV') {
    self.info('Dataset ' + dataset.name + ' does not have a CSV resource.');
    return callback();
  }

  var csvUrl = resource.url;
  var req = request({ url: csvUrl, timeout: self.options.timeout });
  req.on('error', function(err) {
    self.info('Fetching dataset ' + dataset.name + ' timed out.');
    callback();
  });
  req.once('data', function(data) {
    data = data.toString();
    var stopAt = data.indexOf('\n');
    var header = data.substring(0, stopAt).trim();
    var canUse =
      !!header.match(/(^|,)"?'?(kode_provinsi|kode_kabkota)"?'?(,|$)/);

    if (!canUse) {
      self.info('Cannot use dataset ' + dataset.name);
      return callback();
    }

    self.added[dataset.name] = true;
    self.addDataset(dataset);

    req.abort();
    callback();
  });
};

DataGoIdMetaDriver.prototype.addDataset = function(dataset) {
  var self = this;

  var id = self.options.graphBase + dataset.name;
  var add = self.addTriple.bind(self, id);

  add(RDF_NS + 'type', BM_NS + 'DriverInstance');
  add(BM_NS + 'enabled', '"true"^^<' + XSD_NS + 'boolean>');
  add(RDFS_NS + 'label', '"' + self.options.titlePrefix + dataset.title + '"');
  add(BM_NS + 'driverName', '"' + self.options.driverName + '"');
  add(BM_NS + 'optionsYAML', '"datasetId: ' + dataset.name + '"');
};

BmDriverBase.handleCLI();