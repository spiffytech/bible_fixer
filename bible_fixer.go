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
    //text := string(b)

    var m map[string]*json.RawMessage
    err = json.Unmarshal(b, &m)
    fmt.Println(err)
    fmt.Println(m)

    var s string
    json.Unmarshal(*m["content"], &s)
    fmt.Println(s)

    node, err := html.Parse(strings.NewReader(s))
    if err!= nil {
        panic(err)
    }

    fmt.Printf("%T(%v)\n", node)
    doc := gq.NewDocumentFromNode(node)

    doc.Find(".verse").Each(func(i int, s *gq.Selection) {
        //fmt.Println()
        //fmt.Println()
        //fmt.Printf("'%s'\n", s.Text())
        if strings.TrimSpace(s.Text()) == "" {
            return
        }

        num, err := strconv.Atoi(s.Find(".label").Text())
        if err!= nil {
            panic(err)
        }

        verse := Verse{num: num, text: s.Text()}
        _ = verse

        fmt.Println(s.Find(".content").Text())
    })
}
