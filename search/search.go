package search

import (
	"log"
	"strconv"

	"github.com/acgshare/acgsh/db"
	"github.com/blevesearch/bleve"
)

var index bleve.Index

func Init() {
	// open a new index
	var err error
	index, err = bleve.Open("acgsh.bleve")
	if err != nil {
		log.Println(err)
		mapping := bleve.NewIndexMapping()
		index, err = bleve.New("acgsh.bleve", mapping)
		if err != nil {
			log.Println(err)
			return
		}
	}

}

func Index(posts *db.ShPosts) {
	b := index.NewBatch()
	for _, sp := range *posts {
		id := db.Ui64tob(sp.Time)
		id = append(id, ":"...)
		id = append(id, sp.N...)
		id = append(id, ":"...)
		id = append(id, strconv.FormatInt(sp.K, 10)...)

		data := struct {
			Title string
		}{
			Title: sp.Title,
		}

		err := b.Index(string(id), data)
		if err != nil {
			log.Printf("Error: search engine Index: %+v %+v\n", id, data)
		}
	}
	err := index.Batch(b)
	if err != nil {
		log.Printf("Error: search engine index.Batch(): %+v \n", posts)
	}
}

func Search(ss string, size, from int) ([][]byte, error) {
	query := bleve.NewQueryStringQuery(ss)
	search := bleve.NewSearchRequestOptions(query, size, from, false)
	searchResults, err := index.Search(search)
	if err != nil {
		log.Println("Error: search engine Search(): %s \n", err)
		return [][]byte{}, err
	}
	//fmt.Println(searchResults)
	ids := [][]byte{}

	for i := 0; i < searchResults.Hits.Len(); i = i + 1 {
		if searchResults.Hits[i] != nil {
			ids = append(ids, []byte(searchResults.Hits[i].ID))
		}
	}

	return ids, nil
}
