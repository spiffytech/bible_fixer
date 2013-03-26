package main

import (
    "database/sql"
    "encoding/json"
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
    
    _ "github.com/mattn/go-sqlite3"
    "github.com/coopernurse/gorp"
    "code.google.com/p/go.net/html"
    gq "github.com/matrixik/goquery"
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

var num_procs = runtime.NumCPU()
var dbmap *gorp.DbMap
var verseWg sync.WaitGroup
var rawVerseWg sync.WaitGroup
var chapterWg sync.WaitGroup
var cache = gocache.New(5*time.Hour, 30*time.Second)
var wordCounts = gocache.New(5*time.Hour, 30*time.Second)
var chaptersIn = make(chan Chapter)
var chaptersOut = make(chan Verse)
var versesIn = make(chan Verse)
var versesOut = make(chan Verse)
var rawVersesIn = make(chan Verse)
var rawVersesOut = make(chan Verse)
var numRawVerseWorkers = num_procs
var numVerseWorkers = num_procs
var numChapterWorkers = num_procs

func checkIsWord(word string) (isWord bool) {
    res, found := cache.Get(word)
    if !found {
        list, err := dbmap.Select(Word{}, "select * from words where word=?", word)
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

func main() {
    runtime.GOMAXPROCS(num_procs)

    db, err := sql.Open("sqlite3", "./fixer.db")
    if err != nil {
        fmt.Println(err)
        return
    }
    defer db.Close()

    dbmap = &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
    dbmap.TraceOn("[gorp]", log.New(os.Stdout, "", log.Lmicroseconds))
    wdb := dbmap.AddTableWithName(Word{}, "words").SetKeys(false, "word")
    _ = wdb

    wsdb := dbmap.AddTableWithName(Wordset{}, "wordsets")
    _ = wsdb

    err = dbmap.CreateTables()
    fmt.Println(err)
    //if err != nil { 
    //    panic(err)
    //}

    dbmap.Exec("create index if not exists wordsIndex on words (word COLLATE NOCASE)")
    dbmap.Exec("delete from wordsets where 1=1")
    dbmap.Exec("update words set wordcount=0")

    b, err := ioutil.ReadFile("words")
    if err != nil { 
        panic(err)
    }
    words := strings.Fields(string(b))

    row := db.QueryRow("select count(*) c from words")
    var c int
    err = row.Scan(&c)
    if err != nil {
        fmt.Println(err)
        return
    }
    fmt.Println(c)

    if c == 0 {
        for _, word := range words {
            fmt.Println(word)
            w := &Word{Word: word, Count: 0}
            err := dbmap.Insert(w)
            if err != nil {
                panic(err)
            }
        }
    }

    rawVerseWg.Add(numRawVerseWorkers)
    for i := 0; i < numChapterWorkers; i++ {
        go processRawVerse();
    verseWg.Add(numVerseWorkers)
    for i := 0; i < numVerseWorkers; i++ {
        go process_verse()
    }
    chapterWg.Add(numChapterWorkers)
    for i := 0; i < numChapterWorkers; i++ {
        go parseChapter();
    }
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
        verseWg.Wait()
        close(versesOut)
    }()
    go func() {
        for verse := range versesOut {
            _ = verse
        }
    }()

    dir := "./trans/gwt"
    files, _ := ioutil.ReadDir(dir)
    for _, filename := range files {
        if filename.Name() != "2COR.2.gwt" {
            continue
        }

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
        //_ = path.Join
        //_ = files
        //parseChapter("trans/gwt/2COR.2.gwt");
    }
    close(chaptersIn)
    chapterWg.Wait()
    close(chaptersOut)
}


func parseChapter() {
    defer chapterWg.Done()

    for chapter := range chaptersIn {
        filename := chapter.path
        b, err := ioutil.ReadFile(filename)

        var m map[string]*json.RawMessage
        err = json.Unmarshal(b, &m)
        fmt.Println(err)
        //fmt.Println(m)

        var s string
        json.Unmarshal(*m["content"], &s)
        //fmt.Println(s)

        node, err := html.Parse(strings.NewReader(s))
        if err != nil {
            panic(err)
        }

        doc := gq.NewDocumentFromNode(node)
        verses := doc.Find(".verse")
        verses.Each(func(i int, s *gq.Selection) {
            chaptersOut <- Verse{book: chapter.book, chapter: chapter.chapter, html: s}
        })

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
        dbmap.Exec("delete from wordsets where (select wordcount from words where word=word) >= 5")
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

        text = strings.ToLower(text)

        text = strings.Replace(text, "’", "'", -1)

        reg = regexp.MustCompile("'$")
        text = reg.ReplaceAllString(text, "")

        reg = regexp.MustCompile("[^a-zA-Z0-9' -]")
        text = reg.ReplaceAllString(text, "")

        verse.text = text
        verse.words = strings.Fields(text)

        rawVersesOut <- verse
    }
}


func process_verse() {
    defer verseWg.Done()

    for verse := range versesIn {
        var isWord bool
        var err error

        for i, word := range verse.words {
            if strings.HasSuffix(word, "'s") {
                reg := regexp.MustCompile("'s$")
                word = reg.ReplaceAllString(word, "")
            }

            isJoinedWord := false
            isWord = checkIsWord(word)
            if isWord == false {
                splitWord := strings.Split(word, "")
                for letter := 1; letter < len(word)-1; letter++ {
                    half1 := strings.Join(splitWord[:letter], "")
                    half2 := strings.Join(splitWord[letter:], "")

                    if checkIsWord(half1) && checkIsWord(half2) {
                        fmt.Printf("Inserting the following words for %s: %s, %s\n", word, half1, half2)
                        wordSet := &Wordset{Word: word, Word1: half1, Word2: half2, Book: verse.book, Chapter: verse.chapter, Verse: verse.num, RawWord: verse.rawWords[i], Text: verse.text, RawText: verse.rawText}
                        err = dbmap.Insert(wordSet)
                        if err != nil {
                            panic(err)
                        }
                        isJoinedWord = true
                    }   

                }
                if isJoinedWord == false {
                    fmt.Println("Adding " + word + " to the DB")
                    w := &Word{Word: word, Count: 0}
                    fmt.Println(w)
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
