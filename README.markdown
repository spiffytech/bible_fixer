Bible Fixer
==========

The YouVersion Bible texts include import errors where some words don'thave spaces betweenthem like they should. This program finds all such cojoined words, identifies the two correct words, and outputs a CSV of string replacements that is easy to machine-parse, in the hopes YouVersion will take my program output and fix this so it stops bugging me :p

There are two parts to this- the downoader script (writter in Node.js because I thought Node was neat then, and didn't want to rewrite it), and the program that parses all the downloaded Bible texts and does the real work, written in Go.

I made the processing program as accurate as I could, but some real words cannot be distinguished from cojoined words programatically. I tried to scrub these manually, but didn't check all 5,800 replacements :p

This is designed to be used with the God's Word translation. Other translations include HTML elements (footnotes, etc.) that I didn't bother to filter out because GWT doesn't have them, so you'll wind up with bogus things smattered around the verse text this program tries to process. Should be an easy thing to resolve if you cared.

Install
-------

`npm install` should take care of the downloader. 

`go install; go build bible_fixer.go` should take care of the processing program.
