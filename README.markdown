Bible Fixer
==========

The YouVersion Bible texts include import errors where some words don'thave spaces betweenthem like they should. This program finds all such cojoined words, identifies the two correct words, and outputs a CSV of string replacements that is easy to machine-parse, in the hopes YouVersion will take my program output and fix this so it stops bugging me :p

There are two parts to this- the downoader script (writter in Node.js because I thought Node was neat then, and didn't want to rewrite it), and the program that parses all the downloaded Bible texts and does the real work, written in Go.
