package main

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/creachadair/cityhash"
	"github.com/d4l3k/go-pbzip2"
	"github.com/pkg/errors"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	indexFile = flag.String("index", "/home/user/enwiki-20220101-pages-articles-multistream-index.txt.bz2",
		"the index file to load")
	articlesFile = flag.String("articles", "/home/user/enwiki-20220101-pages-articles-multistream.xml.bz2",
		"the article dump file to load")
	search          = flag.Bool("search", false, "whether or not to build a search index")
	searchIndexFile = flag.String("searchIndex", "/home/user/index.bleve", "the search index file")
	httpAddr        = flag.String("http", ":8080", "the address to bind HTTP to")
)

type indexEntry struct {
	id, seek int
}

var mu = struct {
	sync.Mutex

	offsets    map[uint64]indexEntry
	offsetSize map[int]int
}{
	offsets:    map[uint64]indexEntry{},
	offsetSize: map[int]int{},
}
var index bleve.Index

func loadIndex() error {
	mapping := bleve.NewIndexMapping()
	os.RemoveAll(*searchIndexFile)
	var err error
	index, err = bleve.New(*searchIndexFile, mapping)
	if err != nil {
		return err
	}
	f, err := os.Open(*indexFile)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := pbzip2.NewReader(f)
	if err != nil {
		return err
	}
	defer r.Close()

	scanner := bufio.NewScanner(r)

	log.Printf("Reading index file...")
	i := 0
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ":")
		if len(parts) < 3 {
			return errors.Errorf("expected at least 3 parts, got: %#v", parts)
		}
		seek, err := strconv.Atoi(parts[0])
		if err != nil {
			return err
		}
		id, err := strconv.Atoi(parts[1])
		if err != nil {
			return err
		}
		title := strings.Join(parts[2:], ":")
		entry := indexEntry{
			id:   id,
			seek: seek,
		}
		titleHash := cityhash.Hash64([]byte(title))

		mu.Lock()
		mu.offsets[titleHash] = entry
		mu.offsetSize[entry.seek]++
		mu.Unlock()

		i++
		if i%100000 == 0 {
			log.Printf("read %d entries", i)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	log.Printf("Done reading!")

	if !*search {
		return nil
	}
	return nil
}

type redirect struct {
	Title string `xml:"title,attr"`
}

type page struct {
	XMLName    xml.Name   `xml:"page" json:"xml"`
	Title      string     `xml:"title" json:"title"`
	NS         int        `xml:"ns" json:"ns"`
	ID         int        `xml:"id" json:"id"`
	Redirect   []redirect `xml:"redirect" json:"redirects"`
	RevisionID string     `xml:"revision>id" json:"revisionID"`
	Timestamp  string     `xml:"revision>timestamp" json:"timestamp"`
	Username   string     `xml:"revision>contributor>username" json:"-"`
	UserID     string     `xml:"revision>contributor>id" json:"-"`
	Model      string     `xml:"revision>model" json:"model"`
	Format     string     `xml:"revision>format" json:"format"`
	Text       string     `xml:"revision>text" json:"text"`
}

func readArticle(meta indexEntry) (page, error) {
	f, err := os.Open(*articlesFile)
	if err != nil {
		return page{}, err
	}
	defer f.Close()

	mu.Lock()
	maxTries := mu.offsetSize[meta.seek]
	mu.Unlock()

	r := bzip2.NewReader(f)

	if _, err := f.Seek(int64(meta.seek), 0); err != nil {
		return page{}, err
	}

	d := xml.NewDecoder(r)

	var p page
	for i := 0; i < maxTries; i++ {
		if err := d.Decode(&p); err != nil {
			return page{}, err
		}
		if p.ID == meta.id {
			return p, nil
		}
	}

	return page{}, errors.Errorf("failed to find page after %d tries", maxTries)
}

func fetchArticle(name string) (indexEntry, error) {
	mu.Lock()
	defer mu.Unlock()

	articleMeta, ok := mu.offsets[cityhash.Hash64([]byte(name))]
	if ok {
		return articleMeta, nil
	}
	articleMeta, ok = mu.offsets[cityhash.Hash64([]byte(strings.Title(strings.ToLower(name))))]
	if ok {
		return articleMeta, nil
	}
	return indexEntry{}, statusErrorf(http.StatusNotFound, "article not found: %q", name)
}

func randomArticleHash() (uint64, error) {
	mu.Lock()
	defer mu.Unlock()

	for hash := range mu.offsets {
		return hash, nil
	}
	return 0, errors.Errorf("no articles")
}

func randomArticle() (page, error) {
	hash, err := randomArticleHash()
	if err != nil {
		return page{}, err
	}

	mu.Lock()
	meta := mu.offsets[hash]
	mu.Unlock()

	return readArticle(meta)
}

type statusError int

func (s statusError) Error() string {
	return fmt.Sprintf("%d - %s", int(s), http.StatusText(int(s)))
}

func statusErrorf(code int, str string, args ...interface{}) error {
	return errors.Wrapf(statusError(code), str, args...)
}

func main() {
	if err := run(); err != nil {
		log.Printf("%+v\n", err)
	}
}

func run() error {
	flag.Parse()
	log.SetFlags(log.Flags() | log.Lshortfile)

	go func() {
		if err := loadIndex(); err != nil {
			log.Printf("%+v\n", err)
		}
	}()

	http.HandleFunc("/search", func(writer http.ResponseWriter, request *http.Request) {
		q := request.URL.Query().Get("q")
		article, err := fetchArticle(q)
		if err != nil {
			return
		}

		pg, err := readArticle(article)
		if err != nil {
			return
		}
		// //
		// convert, err := wikitext.Convert([]byte(pg.Text))
		// if err != nil {
		// 	return
		// }
		// pg.Text = string(convert)
		// pg.Text = string(convert)
		marshal, err := json.Marshal(pg)
		if err != nil {
			return
		}

		writer.Header().Set("Access-Control-Allow-Origin", "*")
		_, err = writer.Write(marshal)
		if err != nil {
			return
		}
	})

	log.Printf("Listening on %s...", *httpAddr)
	return http.ListenAndServe(*httpAddr, nil)
}
