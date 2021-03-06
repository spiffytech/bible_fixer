package main

import (
    "database/sql"
    "encoding/json"
    "encoding/csv"
    "flag"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "path"
    "regexp"
    "runtime"
    "strconv"
    "strings"
    "sync"
    "time"

     _ "github.com/bmizerany/pq"
    "github.com/coopernurse/gorp"
    "golang.org/x/net/html"
    gq "github.com/PuerkitoBio/goquery"
    gocache "github.com/pmylund/go-cache"
)

type Word struct {
    Word string `db:"word"`
    Count int `db:"wordcount"`
}

type Wordset struct {
    RawWord string `db:"rawWord"`
    Word string `db:"word"`
    Word1 string `db:"word1"`
    Word2 string `db:"word2"`
    Book string `db:"book"`
    Chapter int `db:"chapter"`
    Verse int `db:"verse"`
    Text string `db:"text"`
    RawText string `db:"rawText"`
    Winner bool `db:"winner"`
}

type Chapter struct {
    book string
    chapter int
    path string
}

type Verse struct {
    book string
    chapter int
    num int
    text string
    rawText string
    words []string
    rawWords []string
    html *gq.Selection
}

type Error struct {
    Msg string
}

func (e *Error) Error() string {
    return e.Msg
}

var debug bool
var num_procs = runtime.NumCPU()
var dbmap *gorp.DbMap

var cache = gocache.New(5*time.Hour, 30*time.Second)
var wordCounts = gocache.New(5*time.Hour, 30*time.Second)

var verseWg sync.WaitGroup
var rawVerseWg sync.WaitGroup
var chapterWg sync.WaitGroup

var chaptersIn = make(chan Chapter)
var chaptersOut = make(chan Verse)
var versesIn = make(chan Verse)
var versesOut = make(chan Verse)
var rawVersesIn = make(chan Verse)
var rawVersesOut = make(chan Verse)

var numRawVerseWorkers = num_procs
var numVerseWorkers = num_procs
var numChapterWorkers = num_procs

func main() {
    runtime.GOMAXPROCS(num_procs)

    flag.BoolVar(&debug, "debug", false, "Print extra output for debugging purposes")

    flag.Parse()

    args := flag.Args()
    if len(args) == 0 {
        panic("You must specify a directory that contains your Bible text files")
    }
    dir := args[0]

    db, err := sql.Open("postgres", "user=postgres password=postgres dbname=biblefixer")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    dbmap = &gorp.DbMap{Db: db, Dialect: gorp.PostgresDialect{}}

    if debug {
        dbmap.TraceOn("[gorp]", log.New(os.Stdout, "", log.Lmicroseconds))
    }

    wdb := dbmap.AddTableWithName(Word{}, "words").SetKeys(false, "word")
    _ = wdb

    wsdb := dbmap.AddTableWithName(Wordset{}, "wordsets")
    _ = wsdb

    // PostgreSQL 9.1 introduces "create if not exists". I'm on 8.4 :(
    // Using this instead of CreateTables() begause gorp doesn't presently support 'text' column types
    _, err = dbmap.Exec("create table wordsets ( " + 
            "rawword character varying(255), " +
            "word character varying(255), " +
            "word1 character varying(255), " +
            "word2 character varying(255), " +
            "book character varying(255), " +
            "chapter integer, " +
            "verse integer, " +
            "text text, " +
            "rawtext text " +
        ");")
    // Don't actually handle the error, it's *usually* just "table already exists", which doesn't matter.
    if debug {
        fmt.Println(err)
    }
    err = dbmap.CreateTables()
    if debug {
        fmt.Println(err)
    }

    b, err := ioutil.ReadFile("words")
    if err != nil { 
        panic(err)
    }
    words := strings.Fields(string(b))

    row := db.QueryRow("select count(*) c from words")
    var c int
    err = row.Scan(&c)
    if err != nil {
        panic(err)
    }

    if c == 0 {
        for _, word := range words {
            word = strings.ToLower(word)
            _, found := cache.Get(word)  // Lowercased words result in duplicates that violate the primary key restraint
            if !found {
                w := &Word{Word: word, Count: 0}
                err := dbmap.Insert(w)
                if err != nil {
                    panic(err)
                }
                cache.Set(word, true, -1)
            }
        }
    }

    dbmap.Exec("create index if not exists wordsindex on words (lower(word));")

    // If a previous script run already found the typoed words, and we're just
    // scoring the results, skip over the typo search
    row = db.QueryRow("select count(*) c from wordsets")
    err = row.Scan(&c)
    if err != nil {
        panic(err)
    }

    if c == 0 {
        processText(dir)
    } else {
        if debug {
            fmt.Println("Don't need to process text")
        }
    }

    // Skip scoring if we're working on printing script results
    row = db.QueryRow("select count(*) c from wordsets where winner=true")
    err = row.Scan(&c)
    if err != nil {
        panic(err)
    }

    if c == 0 {
        scoreWinners()
    } else {
        if debug {
            fmt.Println("Don't need to score winners")
        }
    }

    var finalReplacements [][]string

    writer := csv.NewWriter(os.Stdout)
    writer.Write([]string{"book", "chapter", "verse", "rawWord", "word1", "word2"})

    list, err := dbmap.Select(Wordset{}, "select * from wordsets where winner=true order by book, chapter, verse, word asc")
    for _, word := range list {
        wordSet := word.(*Wordset)
        unmunged := unmungeWord(wordSet.RawWord, wordSet.Word1, wordSet.Word1)
        stuff := append([]string{wordSet.Book, strconv.Itoa(wordSet.Chapter), strconv.Itoa(wordSet.Verse), wordSet.RawWord}, unmunged...)

        finalReplacements = append(finalReplacements, stuff)
    }

    writer.WriteAll(finalReplacements)
}

func scoreWinners() {
    type wordScores struct {
        Score int
        Word string
        Word1 string
        Word2 string
    }

    list, err := dbmap.Select(wordScores{}, "select wordsets.word, wordscores1.wordcount+wordscores2.wordcount score, word1, word2 from wordsets join (select word, wordcount from words) wordscores1 on wordsets.word1=wordscores1.word join (select word, wordcount from words) wordscores2 on wordsets.word2=wordscores2.word where not rawword ~ '[a-zA-Z][.,:!?]+[^a-z]*[A-Z]' and not wordscores1.wordcount=0 and not wordscores2.wordcount=0 group by wordsets.word, word1, word2, score order by wordsets.word, score desc;")
    if err != nil {
        panic(err)
    }

    var wordReplacements = make(map[string][]string)
    for _, word := range list {
        wordReplacement := word.(*wordScores)
        if _, ok := wordReplacements[wordReplacement.Word]; !ok {
            _, err = dbmap.Exec("update wordsets set winner=true where word=$1 and word1=$2 and word2=$3", wordReplacement.Word, wordReplacement.Word1, wordReplacement.Word2)
            if err != nil {
                panic(err)
            }
            wordReplacements[wordReplacement.Word] = []string{wordReplacement.Word1, wordReplacement.Word2}
        }
    }
}

// Reads Bible text, parses words, finds typoes, counts word frequency
func processText(dir string) {
    list, _ := dbmap.Select(Word{}, "select word from words")
    for _, word := range list {
        word := word.(*Word)
        cache.Set(word.Word, true, -1)
    }

    dbmap.Exec("update words set wordcount=0")

    chapterWg.Add(numChapterWorkers)
    for i := 0; i < numChapterWorkers; i++ {
        go parseChapter();
    }
    rawVerseWg.Add(numRawVerseWorkers)
    for i := 0; i < numChapterWorkers; i++ {
        go processRawVerse();
    }
    verseWg.Add(numVerseWorkers)
    for i := 0; i < numVerseWorkers; i++ {
        go processVerse()
    }

    go func() {
        for verse := range chaptersOut {
            rawVersesIn <- verse
        }
        close(rawVersesIn)
        rawVerseWg.Wait()
        close(rawVersesOut)
    }()
    go func() {
        for verse := range rawVersesOut {
            versesIn <- verse
        }
        close(versesIn)
    }()
    go func() {
        for verse := range versesOut {
            _ = verse
        }
    }()

    files, _ := ioutil.ReadDir(dir)
    for _, filename := range files {
        path := path.Join(dir, filename.Name())

        regex := regexp.MustCompile(`(?P<book>\S+)\.(?P<chapter>\d+).gwt`)
        matches := regex.FindStringSubmatch(filename.Name())
        bookName := matches[1]
        chapterNum, err := strconv.Atoi(matches[2])
        if err!= nil {
            panic(err)
        }

        chapter := Chapter{book: bookName, chapter: chapterNum, path: path}
        chaptersIn <- chapter
    }
    close(chaptersIn)
    chapterWg.Wait()
    close(chaptersOut)

    verseWg.Wait()
    close(versesOut)

    list, err := dbmap.Select(Word{}, "select word from words")
    for _, word := range list {
        word := word.(*Word)
        res, found := wordCounts.Get(word.Word)
        if found {
            word.Count = res.(int)
            _, err = dbmap.Update(word)
            if err != nil {
                panic(err)
            }
        }
    }
}


func checkIsWord(word string) (isWord bool) {
    res, found := cache.Get(word)
    if !found {
        list, err := dbmap.Select(Word{}, "select * from words where word=$1", word)
        if err != nil {
            panic(err)
        }
        isWord = len(list) != 0
        cache.Set(word, isWord, -1)
    } else {
        isWord = res.(bool) == true
    }

    return isWord
}


// Fix artifacts in the text. MS Smart Quotes, trailing quotes, etc.
func mungeWord(word string) string {
    word = strings.ToLower(word)

    word = strings.Replace(word, "’", "'", -1)

    reg := regexp.MustCompile("'$")
    word = reg.ReplaceAllString(word, "")

    reg = regexp.MustCompile("[^a-zA-Z0-9' -]")
    word = reg.ReplaceAllString(word, "")

    word = strings.TrimSpace(word)
    word = strings.Trim(word, "'")

    return word
}


// Attempt to split joined words when they're separated by odd punctuation
func unmungeWord(rawWord, half1, half2 string) ([]string) {
    var rawHalf1 []string
    var rawHalf2 []string

    regex := regexp.MustCompile("[^A-Za-z]")

    chars := strings.Split(rawWord, "")
    for i, char := range chars {
        rawHalf1 = append(rawHalf1, char)
        if mungeWord(strings.Join(rawHalf1, "")) == half1 {
            for _, char := range chars[i+1:] {
                if regex.MatchString(char) && char != "‘" && char != "“" {
                    rawHalf1 = append(rawHalf1, char)
                } else {
                    rawHalf2 = append(rawHalf2, char)
                }
            }

            if len(rawHalf1) == 0 || len(rawHalf2) == 0  {
                panic(fmt.Sprintf("Couldn't unmunge '%s' - %s (%s), %s (%s)", rawWord, half1, strings.Join(rawHalf1, ""), half2, strings.Join(rawHalf2, "")))
            }

            break
        }
    }

    return []string{strings.Join(rawHalf1, ""), strings.Join(rawHalf2, "")}
}


func parseChapter() {
    defer chapterWg.Done()

    for chapter := range chaptersIn {
        filename := chapter.path
        b, err := ioutil.ReadFile(filename)

        var m map[string]*json.RawMessage
        err = json.Unmarshal(b, &m)
        if err != nil {
            panic(err)
        }

        var s string
        json.Unmarshal(*m["content"], &s)

        node, err := html.Parse(strings.NewReader(s))
        if err != nil {
            panic(err)
        }

        doc := gq.NewDocumentFromNode(node)
        verses := doc.Find(".verse")
        verses.Each(func(i int, s *gq.Selection) {
            chaptersOut <- Verse{book: chapter.book, chapter: chapter.chapter, html: s}
        })
    }
}


func processRawVerse() {
    defer rawVerseWg.Done()

    for verse := range rawVersesIn {
        s := verse.html

        if strings.TrimSpace(s.Text()) == "" {  // We get some bad HTML sometimes, indicating an invalid verse. No further processing required.
            continue
        }

        num, err := strconv.Atoi(s.Find(".label").Text())
        if err!= nil {
            panic(err)
        }
        verse.num = num

        text := s.Find(".content").Text()

        reg := regexp.MustCompile(" {2,}")
        text = reg.ReplaceAllString(text, " ")

        verse.rawText = text
        verse.rawWords = strings.Fields(text)

        for _, word := range verse.rawWords {
            verse.words = append(verse.words, mungeWord(word))
        }

        rawVersesOut <- verse
    }
}


func processVerse() {
    defer verseWg.Done()

    for verse := range versesIn {
        var isWord bool
        var err error

        rawWordRegex := regexp.MustCompile("^\\(?[A-Z]")
        wordRegex := regexp.MustCompile("(^[0-9]+$|arand|nebat|shallum|arza|great-great-grandson|tent-like|super-apostles|half-sheet|calf-shaped|non-jews|spring-fed|cross-examines|non-israelites)")
        for i, word := range verse.words {
            if rawWordRegex.MatchString(verse.rawWords[i]) {
                continue
            }
            if wordRegex.MatchString(verse.words[i]) {
                continue
            }

            if strings.HasSuffix(word, "'s") {
                reg := regexp.MustCompile("'s$")
                word = reg.ReplaceAllString(word, "")
            }

            isJoinedWord := false
            isWord = checkIsWord(word)

            if isWord == false && strings.HasSuffix(word, "s") {
                chars := strings.Split(word, "")
                isWord = checkIsWord(strings.Join(chars[:len(chars)-1], ""))
            }

            if isWord == false {
                splitWord := strings.Split(word, "")
                for letter := 1; letter < len(word); letter++ {
                    half1 := strings.Join(splitWord[:letter], "")
                    half2 := strings.Join(splitWord[letter:], "")

                    if checkIsWord(half1) && checkIsWord(half2) {
                        if debug {
                            fmt.Printf("Inserting the following words for %s: %s, %s\n", word, half1, half2)
                        }
                        wordSet := &Wordset{Word: word, Word1: half1, Word2: half2, Book: verse.book, Chapter: verse.chapter, Verse: verse.num, RawWord: verse.rawWords[i], Text: verse.text, RawText: verse.rawText}
                        err = dbmap.Insert(wordSet)
                        if err != nil {
                            panic(err)
                        }
                        isJoinedWord = true
                    }

                }
                if isJoinedWord == false {
                    if debug {
                        fmt.Println("Adding " + word + " to the DB")
                    }
                    w := &Word{Word: word, Count: 1}
                    if debug {
                        fmt.Println(w)
                    }
                    err := dbmap.Insert(w)
                    if err != nil {
                        panic(err)
                    }
                    cache.Set(word, true, -1)
                    isWord = true
                }
            }

            err = wordCounts.Increment(word, 1)
            if err != nil {
                wordCounts.Set(word, 1, -1)
            }
        }

        versesOut <- verse
    }
}
