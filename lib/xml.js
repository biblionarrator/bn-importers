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
            var stream = fs.createReadStream(filename);
            stream = new XmlStream(stream);
            options.collect.forEach(function (element) {
                stream.collect(element);
            });
            self.emit('filestart', filename);
            stream.on('end', function() {
                self.emit('filefinish', filename, filecount);
            });
            stream.on('endElement: ' + options.recordElement, function (record) {
                var promise = Q.defer();
                promises.push(promise.promise);
                promise.promise.catch(function (record) {
                    rejects.push(record);
                });
                if (filecount % options.commit === 0 && filecount > 0) {
                    stream.pause();
                    Q.all(promises).then(function () {
                        commit(function () {
                            console.log('Committing after ' + filecount);
                            setTimeout(function () {
                                stream.resume();
                            }, 100);
                        });
                    });
                }
                filecount++;
                self.emit('record', record, promise);
            });
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

    processNextFile();
}

util.inherits(XMLImporter, events.EventEmitter);

