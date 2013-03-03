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

var debug = function(str) {
    if(process.env.debug) {
        console.log(str);
    }
}

var trans = null;
if(process.argv.length > 2) {
    trans = process.argv[2].toLowerCase();
    debug("Using translation " + trans);
} else {
    trans = "gwt";
}

var bible_chapters = [
    {book: "Genesis", chapter: 50, abbr: "gen"},
    {book: "Exodus", chapter: 40, abbr: "exo"},
    {book: "Leviticus", chapter: 27, abbr: "lev"},
    {book: "Numbers", chapter: 36, abbr: "num"},
    {book: "Deuteronomy", chapter: 34, abbr: "deu"},
    {book: "Joshua", chapter: 24, abbr: "jos"},
    {book: "Judges", chapter: 21, abbr: "jdg"},
    {book: "Ruth", chapter: 4, abbr: "rut"},
    {book: "1 Samuel", chapter: 31, abbr: "1sa"},
    {book: "2 Samuel", chapter: 24, abbr: "2sa"},
    {book: "1 Kings", chapter: 22, abbr: "1ki"},
    {book: "2 Kings", chapter: 25, abbr: "2ki"},
    {book: "1 Chronicles", chapter: 29, abbr: "1ch"},
    {book: "2 Chronicles", chapter: 36, abbr: "2ch"},
    {book: "Ezra", chapter: 10, abbr: "ezr"},
    {book: "Nehemiah", chapter: 13, abbr: "neh"},
    {book: "Esther", chapter: 10, abbr: "est"},
    {book: "Job", chapter: 42, abbr: "job"},
    {book: "Psalms", chapter: 150, abbr: "psa"},
    {book: "Proverbs", chapter: 31, abbr: "pro"},
    {book: "Ecclesiastes", chapter: 12, abbr: "ecc"},
    {book: "Song of Songs", chapter: 8, abbr: "sng"},
    {book: "Isaiah", chapter: 66, abbr: "isa"},
    {book: "Jeremiah", chapter: 52, abbr: "jer"},
    {book: "Lamentations", chapter: 5, abbr: "lam"},
    {book: "Ezekiel", chapter: 48, abbr: "ezk"},
    {book: "Daniel", chapter: 12, abbr: "dan"},
    {book: "Hosea", chapter: 14, abbr: "hos"},
    {book: "Joel", chapter: 3, abbr: "jol"},
    {book: "Amos", chapter: 9, abbr: "amo"},
    {book: "Obadiah", chapter: 1, abbr: "oba"},
    {book: "Jonah", chapter: 4, abbr: "jon"},
    {book: "Micah", chapter: 7, abbr: "mic"},
    {book: "Nahum", chapter: 3, abbr: "nam"},
    {book: "Habakkuk", chapter: 3, abbr: "hab"},
    {book: "Zephaniah", chapter: 3, abbr: "zep"},
    {book: "Haggai", chapter: 2, abbr: "hag"},
    {book: "Zechariah", chapter: 14, abbr: "zec"},
    {book: "Malachi", chapter: 4, abbr: "mal"},
    {book: "Matthew", chapter: 28, abbr: "mat"},
    {book: "Mark", chapter: 16, abbr: "mrk"},
    {book: "Luke", chapter: 24, abbr: "luk"},
    {book: "John", chapter: 21, abbr: "jhn"},
    {book: "Acts", chapter: 28, abbr: "act"},
    {book: "Romans", chapter: 16, abbr: "rom"},
    {book: "1 Corinthians", chapter: 16, abbr: "1co"},
    {book: "2 Corinthians", chapter: 13, abbr: "2cor"},
    {book: "Galatians", chapter: 6, abbr: "gal"},
    {book: "Ephesians", chapter: 6, abbr: "eph"},
    {book: "Philippians", chapter: 4, abbr: "php"},
    {book: "Colossians", chapter: 4, abbr: "col"},
    {book: "1 Thessalonians", chapter: 5, abbr: "1th"},
    {book: "2 Thessalonians", chapter: 3, abbr: "2th"},
    {book: "1 Timothy", chapter: 6, abbr: "1ti"},
    {book: "2 Timothy", chapter: 4, abbr: "2ti"},
    {book: "Titus", chapter: 3, abbr: "tit"},
    {book: "Philemon", chapter: 1, abbr: "phm"},
    {book: "Hebrews", chapter: 13, abbr: "heb"},
    {book: "James", chapter: 5, abbr: "jas"},
    {book: "1 Peter", chapter: 5, abbr: "1pe"},
    {book: "2 Peter", chapter: 3, abbr: "2pe"},
    {book: "1 John", chapter: 5, abbr: "1jn"},
    {book: "2 John", chapter: 1, abbr: "2jn"},
    {book: "3 John", chapter: 1, abbr: "3jn"},
    {book: "Jude", chapter: 1, abbr: "jud"},
    {book: "Revelation", chapter: 22, abbr: "rev"},
];

var get_chapter = function(book, chapter, cb) {
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
}


step(
    function() {
        var group = this.group();
        for(var book in bible_chapters) {
            if(!bible_chapters.hasOwnProperty(book)) continue;

            for(var i = 1; i <= bible_chapters[book]["chapter"]; i++) {
                get_chapter(bible_chapters[book]["abbr"].toUpperCase(), i, group());
            }
        }
    }, function(err, results) {
        if(err) {
            console.log("Error getting chapters: " + err.toString());
        }
        debug("Items recorded: " + results.length);
    }
);
