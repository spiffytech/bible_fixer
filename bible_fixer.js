"use strict";

var step = require("step");
var fs = require("fs");

var request = require("request");

var _ = require("underscore");

var _s = require("underscore.string");

var jQuery = require("jQuery");
//var $ = require('jQuery');
//var jq = require('jQuery').create();

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

var memoize = require("memoizee");


var trans = null;
if(process.argv.length > 2) {
    trans = process.argv[2].toLowerCase();
    debug("Using translation " + trans);
} else {
    trans = "gwt";
}

var dict = {};
var wordlist = [];
var wordpairs = {};

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


var munge_word = function(word) {
	return word.toLowerCase().replace("’", "'", "g").replace(/'$/g, "").replace(/[^a-zA-Z0-9'-]/g, "");
}


var count_words = function(book, chapter, verse, verse_text, cb) {
	var words = jQuery(verse_text).find(".content").text().replace(/ {2,}/g, "").split(" ");
	for(var word = 0; word < words.length; word++) {
		var w = munge_word(words[word]);
		wordlist.push(w)
	}
	wordlist = _.uniq(wordlist);

	cb();
}


function process_chapter(book, chapter, op, cb) {
	step(
		function() {
			fs.readFile(_s.sprintf("trans/%s/%s.%s.%s", trans, book.toUpperCase(), chapter, trans), this);
		}, function(err, data) {
			if(err) {
				if(err.code === "EMFILE") {
					debug("Too many files, retrying: " + _s.sprintf("%s %s", book, chapter));
					setTimeout(get_contents, (Math.random * 11) * 1000, book, chapter, cb);
					return;
				}

				console.log("Error reading file: " + err.toString());
				process.exit(1);
			}

			data = JSON.parse(data.toString());
			data = data["content"];

			var jsdom = require("jsdom").jsdom;
			var myWindow = jsdom().createWindow();
			var jQuery = require("jQuery").create(myWindow);
			jQuery(data).appendTo("body");

			var group = this.group();
			var verses = jQuery(".verse");

			for(var i = 0; i < verses.length; i++) {
				if(!verses.hasOwnProperty(i)) continue;

				var verse = jQuery(verses[i]);
				var verse_num = parseInt(jQuery(verse).find(".label").text());
				op(book, chapter, verse_num, verses[i], group());
			}
		}, function(err, results) {
			cb();
		}
	);
}


var refresh_wordlist = function(cb) {
	step(
		function() {
			var group = this.group();
			for(var book in bible_chapters) {
				if(book > 1) return;
				if(!bible_chapters.hasOwnProperty(book)) continue;

				for(var i = 1; i <= bible_chapters[book]["chapter"]; i++) {
					process_chapter(bible_chapters[book]["abbr"], i, count_words, group());
				}
			}
		}, function(err, results) {
			fs.writeFile("wordlist", JSON.stringify(wordlist));
			cb();
		}
	);
}


var generate_word_pairs = function(cb) {
	for(var word1 in wordlist) {
		if(!wordlist.hasOwnProperty(word1)) continue;

		for(var word2 in wordlist) {
			if(!wordlist.hasOwnProperty(word2)) continue;

			wordpairs[word1 + word2] = true;
		}
	}

	fs.writeFile("wordpairs", JSON.stringify(wordpairs), cb);
}


var identify_typos = function(cb) {
	step(
		function() {
			var group = this.group();
			for(var book in bible_chapters) {
				if(book > 1) return;
				if(!bible_chapters.hasOwnProperty(book)) continue;

				for(var i = 1; i <= bible_chapters[book]["chapter"]; i++) {
					process_chapter(bible_chapters[book]["abbr"], i, function(book, chapter, verse, verse_text, cb) {
						var words = jQuery(verse_text).find(".content").text().replace(/ {2,}/g, "").split(" ");
						for(var word in words) {
							if(!words.hasOwnProperty(word)) continue;

							var w = words[word];
							if(dict[w] === undefined && wordpairs[w] === true) {
								console.log("Joined word: " + w);
							}
						}
					}, group());
				}
			}
		}, function(err, results) {
			cb();
		}
	);
}


step(
	function() {
		fs.readFile("words", this);
	}, function(err, text) {
		text = text.toString().split("\n");
		for(var word in text) {
			dict[text[word]] = true;
		}

		fs.exists("wordlist", this);
	}, function(exists) {
		if(!exists) {
			refresh_wordlist(this);
		} else {
			debug("Using cached word list")
			var that = this;
			step(
				function() {
					fs.readFile("wordlist", this);
				}, function(err, data) {
					wordlist = JSON.parse(data.toString());
					that();
				}
			);
		}
	}, function() {
		console.log(wordlist.length + " words");

		fs.exists("wordpairs", this);
	}, function(exists) {
		if(!exists) {
			generate_word_pairs(this);
		} else {
			debug("Using cached wordpair list");
			var that = this;
			step(
				function() {
					fs.readFile("wordpairs", this);
				}, function(err, data) {
					if(err) {
						console.log("Error reading wordpairs: " + err.toString());
						process.exit(1);
					}

					wordpairs = JSON.parse(data.toString());
					that();
				}
			);
		}
	}, function() {
		console.log(Object.keys(wordpairs).length + " word pairs");
		
		identify_typos();
	}
);
