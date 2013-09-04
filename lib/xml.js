var events = require('events'),
    fs = require('fs'),
    util = require('util'),
    XmlStream = require('xml-stream'),
    Q = require('q');

module.exports = XMLImporter;

function XMLImporter(options) {
    var files = options.files;
    options.collect = options.collect || [ ];
    var self = this;
    var promises = [ ];
    events.EventEmitter.call(this);

    function processNextFile() {
        if (files.length > 0) {
            self.filerecords = 0;
            self.filename = files.shift();
            console.log(self.filename);
            var stream = fs.createReadStream(self.filename);
            self.stream = new XmlStream(stream);
            options.collect.forEach(function (element) {
                self.stream.collect(element);
            });
            self.emit('filestart');
            self.stream.on('end', function() {
                self.emit('filefinish');
            });
            self.stream.on('endElement: ' + options.recordElement, function (record) {
                var promise = Q.defer();
                promises.push(promise.promise);
                if (self.filerecords % 1000 === 0 && self.filerecords > 0) {
                    self.stream.pause();
                    Q.all(promises).then(function () {
                        var commitPromise = Q.defer();
                        self.emit('commit', commitPromise);
                        promises = [ ];
                        commitPromise.promise.then(function () {
                            setTimeout(function () {
                                self.stream.resume();
                            }, 100);
                        });
                    });
                }
                self.filerecords++;
                self.emit('record', record, promise);
            });
            return true;
        } else {
            return false;
        }
    }

    this.on('filefinish', function() {
        var commitPromise = Q.defer();
        self.emit('commit', commitPromise);
        commitPromise.promise.then(function () {
            console.log("Processed " + self.filerecords + " records in " + self.filename);
            setTimeout(function () {
                if (!processNextFile()) {
                    self.emit('done');
                }
            }, 100);
        });
    });

    processNextFile();
}

util.inherits(XMLImporter, events.EventEmitter);

