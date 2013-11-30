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
    var rejects = [ ];
    var total = 0;
    events.EventEmitter.call(this);

    function commit(callback) {
        Q.allSettled(promises).then(function () {
            var commitPromise = Q.defer();
            self.emit('commit', commitPromise);
            promises = [ ];
            commitPromise.promise.finally(callback);
        });
    }

    function processNextFile() {
        if (files.length > 0) {
            var filecount = 0;
            var filename = files.shift();
            var parser = new JsonParser();
            var stream = fs.createReadStream(filename);
            stream.on('error', function(err) {
                console.log(err);
            });
            stream.on('data', function (chunk) {
                parser.write(chunk);
            });
            self.emit('filestart', filename);
            stream.on('end', function() {
                self.emit('filefinish', filename, filecount);
            });
            parser.onValue = function (record) {
                if (this.stack.length === 0) {
                    var promise = Q.defer();
                    promises.push(promise.promise);
                    promise.promise.catch(function (record) {
                        rejects.push(record);
                    });
                    if (filecount % options.commit === 0 && filecount > 0) {
                        stream.pause();
                        commit(function () {
                            console.log('Committing after ' + filecount);
                            setTimeout(function () {
                                stream.resume();
                            }, 100);
                        });
                    }
                    filecount++;
                    self.emit('record', record, promise);
                }
            };
            return true;
        } else {
            return false;
        }
    }

    this.on('filefinish', function(filename, count) {
        commit(function () {
            console.log("Processed " + count + " records in " + filename);
            setTimeout(function () {
                if (!processNextFile()) {
                    self.emit('done', rejects);
                }
            }, 100);
        });
    });

    if (!options.pause) {
        processNextFile();
    } else {
        this.start = processNextFile;
    }
}

util.inherits(JSONImporter, events.EventEmitter);


