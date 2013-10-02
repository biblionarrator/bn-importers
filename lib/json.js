var events = require('events'),
    fs = require('fs'),
    util = require('util'),
    JsonParser = require('jsonparse'),
    Q = require('q');

module.exports = JSONImporter;

function JSONImporter(options) {
    var files = options.files;
    var self = this;
    var promises = [ ];
    events.EventEmitter.call(this);

    function processNextFile() {
        if (files.length > 0) {
            self.filerecords = 0;
            self.filename = files.shift();
            self.parser = new JsonParser();
            self.stream = fs.createReadStream(self.filename);
            self.stream.on('error', function(err) {
                console.log(err);
            });
            self.stream.on('data', function (chunk) {
                self.parser.write(chunk);
            });
            self.emit('filestart');
            self.stream.on('end', function() {
                self.emit('filefinish');
            });
            self.parser.onValue = function (record) {
                if (this.stack.length === 0) {
                    var promise = Q.defer();
                    promises.push(promise.promise);
                    if (self.filerecords % 1000 === 0 && self.filerecords > 0) {
                        self.stream.pause();
                        Q.all(promises).then(function () {
                            console.log('Committing after ' + self.filerecords);
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
                }
            };
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

util.inherits(JSONImporter, events.EventEmitter);


