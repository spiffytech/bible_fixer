package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
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

type Verse struct {
    book string
    chapter int
    num int
    text string
    rawText string
    words []string
    rawWords []string
}

var num_procs = runtime.NumCPU()
var dbmap *gorp.DbMap
var wg2 sync.WaitGroup
var cache = gocache.New(5*time.Hour, 30*time.Second)
var finalProgress = make(chan Verse)

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
    wdb := dbmap.AddTableWithName(Word{}, "words")
    _ = wdb

    wsdb := dbmap.AddTableWithName(Wordset{}, "wordsets")
    _ = wsdb

    err = dbmap.CreateTables()
    fmt.Println(err)
    //if err != nil { 
    //    panic(err)
    //}

    dbmap.Exec("create index if not exists wordsIndex on words (word)")

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

    go process_verse()

    if c == 0 {
        for _, word := range words {
            fmt.Println(word)
            w := &Word{word}
            err := dbmap.Insert(w)
            if err != nil {
                panic(err)
            }
        }
    }

    parse_file("trans/gwt/2COR.2.gwt");
}

func process_verse() {
    defer wg2.Done()

    for verse := range finalProgress {
        for i, word := range verse.words {
            isJoinedWord := false
            isWord, found := cache.Get(word)
            if !found {
                list, err := dbmap.Select(Word{}, "select * from words where word=?", word)
                if err != nil {
                    panic(err)
                }
                isWord = len(list) != 0
                cache.Set(word, isWord, -1)
            }

            if isWord  == false {
                splitWord := strings.Split(word, "")
                for letter := 1; letter < len(word)-1; letter++ {
                    half1 := strings.Join(splitWord[:letter], "")
                    half2 := strings.Join(splitWord[letter:], "")
                    fmt.Println("For word " + word + ": " + half1 + ", " + half2)

                    list, err := dbmap.Select(Word{}, "select words1.w || words2.w word from (select word w from words where word like ?) words1 cross join (select word w from words where word like ?) words1 where word=?", half1 + "%", "%" + half2, word)
                    if err != nil {
                        panic(err)
                    }

                    if len(list) != 0 {
                        fmt.Printf("Inserting the following words for %s: %s, %s", word, half1, half2)
                        //
                        //
                        // Need to add verse text as well, for post-searching "which word pair is this" decision purposes
                        // Also, this needs to be in a loop
                        //
                        //
                        wordSet := &Wordset{Word: word, Word1: half1, Word2: half2, Book: verse.book, Chapter: verse.chapter, Verse: verse.num, RawWord: verse.rawWords[i]}
                        err = dbmap.Insert(wordSet)
                        if err != nil {
                            panic(err)
                        }
                        isJoinedWord = true
                    }
                }
                if isJoinedWord == true {
                    fmt.Println("Adding " + word + " to the DB")
                    w := &Word{word}
                    err := dbmap.Insert(w)
                    if err != nil {
                        panic(err)
                    }
                    cache.Set(word, true, -1)
                }
            }
        }
    }
}

func parse_file(filename string) {
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

    var wg sync.WaitGroup
    num_verses := len(verses.Nodes)
    fmt.Println("num verses = " + strconv.Itoa(num_verses))

    versesIn := make(chan *gq.Selection)
    versesOut := make(chan Verse)

    wg.Add(num_procs)
    for i := 0; i < num_procs; i++ {
        go func() {
            defer wg.Done()

            //fmt.Printf("'%s'\n", s.Text())
            for s := range versesIn {
                if strings.TrimSpace(s.Text()) == "" {  // We get some bad HTML sometimes, indicating an invalid verse. No further processing required.
                    continue
                }

                verse := Verse{}

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

                //fmt.Println(verse)
                versesOut <- verse
            }
        }()
    }

    go func() {
        verses.Each(func(i int, s *gq.Selection) {
            versesIn <- s
        })

        close(versesIn)
        wg.Wait()
        close(versesOut)
    }()

    for verse := range versesOut {
        fmt.Printf("Processing verse %d\n", verse.num)
        wg2.Add(1)
        finalProgress <- verse
        //fmt.Printf("Finished processing verse %d\n", vss[len(vss)-1].num)
    }
    wg2.Wait()
}
