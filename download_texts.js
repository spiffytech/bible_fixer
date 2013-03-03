"use strict";

var step = require("step");
var fs = require("fs");

var request = require("request");

var _s = require("underscore.string");

var LRU = require("lru-cache");
var cache = LRU({
    max: 1024 * 1024 * 15,
    length: function(val) {return JSON.stringify(val).length;},
    maxAge: 1000 * 60
});

var trans = "gwt";

var debug = function(str) {
    if(process.env.debug) {
        console.log(str);
    }
}

step(
    function() {
        var group = this.group();
        for(var i = 1; i < 5; i++) {
            (function get_chapter(book, chapter, cb) {
                var dirname = trans;
                var filename = _s.sprintf("%s/%s.%s.%s", trans, book, chapter, trans);
                step(
                    function() {
                        fs.exists(dirname, this);
                    }, function(exists) {
                        if(!exists) {
                            fs.mkdir(dirname, this);
                        } else {
                            this();
                        }
                    }, function() {
                        fs.exists(filename, this);
                    }, function(exists) {
                        if(exists === true) {
                            debug("File exists: " + filename);
                            cb();
                        } else {
                            debug("File doesn't exist, downloading: " + filename);
                            request(
                                "https://www.youversion.com/bible/416/" + book + "." + chapter + ".json", 
                                this
                            );
                        }
                    }, function(err, response, body) {
                        if(response.statusCode != 200) {
                            console.log("Retrying: " + filename);
                            setTimeout(get_chapter, 30 + (Math.random() * 10), book, chapter, cb);
                            return;
                        }

                        debug("Writing: " + filename);
                        fs.writeFile(filename, body, function(err) {
                            cb(err, null);
                        });
                    }
                );
            })("PRO", i, group());
        }
    }, function(err, results) {
        if(err) {
            console.log("Error getting chapters: " + err.toString());
        }
        debug("Items written");
        console.log(results);
        console.log(results.length);
    }
);
