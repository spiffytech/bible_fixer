package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "runtime"
    "strconv"
    "strings"
    "sync"
    
    _ "github.com/mattn/go-sqlite3"
    "github.com/coopernurse/gorp"
    "code.google.com/p/go.net/html"
    gq "github.com/matrixik/goquery"
)

type Word struct {
    Word string
}

type Wordpair struct {
    Wordpair string
}

type Wordset struct {
    Word *Word
    Words string
}

type Verse struct {
    book string
    chapter int
    num int
    text string
}


func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    db, err := sql.Open("sqlite3", "./wordpairs.db")
    if err != nil {
        fmt.Println(err)
        return
    }
    defer db.Close()

    dbmap := &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
    dbmap.TraceOn("[gorp]", log.New(os.Stdout, "biblefixer:", log.Lmicroseconds))
    wdb := dbmap.AddTableWithName(Word{}, "words")
    _ = wdb
    wpdb := dbmap.AddTableWithName(Wordpair{}, "wordpairs")
    _ = wpdb
    wsdb := dbmap.AddTableWithName(Wordset{}, "wordsets")
    _ = wsdb
    dbmap.CreateTables()

    b, err := ioutil.ReadFile("words")
    if err != nil { panic(err) }
    words := strings.Fields(string(b))

    row := db.QueryRow("select count(*) c from word")
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
            w := &Word{word}
            err := dbmap.Insert(w)
            fmt.Println(err)
        }
    }

    parse_file("trans/gwt/2COR.2.gwt");
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
    if err!= nil {
        panic(err)
    }

    doc := gq.NewDocumentFromNode(node)

    verses := doc.Find(".verse")

    var wg sync.WaitGroup
    num_verses := len(verses.Nodes)
    wg.Add(num_verses);
    fmt.Println("num verses = " + strconv.Itoa(num_verses))

    versesOut := make(chan Verse)

    process_verse := func(i int, s *gq.Selection) {
        defer wg.Done()
        //fmt.Println()
        //fmt.Println()
        //fmt.Printf("'%s'\n", s.Text())
        if strings.TrimSpace(s.Text()) == "" {  // We get some bad HTML sometimes, indicating an invalid verse. No further processing required.
            return
        }

        num, err := strconv.Atoi(s.Find(".label").Text())
        if err!= nil {
            panic(err)
        }

        verse := Verse{num: num, text: s.Find(".content").Text()}
        fmt.Println(verse)
        versesOut <- verse
    };
    verses.Each(func(i int, s *gq.Selection) {
        go process_verse(i, s)
    })

    go func() {
        wg.Wait()
        close(versesOut)
    }()

    vss := make([]Verse, 0, num_verses)
    for newVerse := range versesOut {
        fmt.Printf("len vss = %d\n", len(vss))
        vss = append(vss, newVerse)
        fmt.Println(vss[len(vss)-1].num)
    }
    fmt.Println("here")
}
