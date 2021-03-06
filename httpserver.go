package main

import (
	"encoding/json"
	"github.com/acgshare/acgsh/db"
	"github.com/acgshare/acgsh/rpc"
	"github.com/acgshare/acgsh/search"
	"log"
	"net/http"
	"strconv"
	"strings"
)

const (
	posts_per_page = 80
)

var httpPort string = "8080"

func handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	myItems := []string{r.Proto, "item2", r.URL.Path[1:]}
	a, _ := json.Marshal(myItems)

	w.Write(a)

}
func homeHandler(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Path[1:]) == 0 {
		http.ServeFile(w, r, "acgsh_html/index.html")
		return
	}
	http.ServeFile(w, r, "acgsh_html/"+r.URL.Path[1:])

}
func getPostsHandler(w http.ResponseWriter, r *http.Request) {
	ss := r.URL.Path[11:]
	u, _ := strconv.ParseUint(ss, 10, 32)
	data, err := db.GetPosts(uint(u)*posts_per_page, posts_per_page)
	if err != nil {
		log.Println("Error: getPostsHandler", err)
		http.Error(w, "getPostsHandler db err", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(data)

}
func getCategoryPostsHandler(w http.ResponseWriter, r *http.Request) {
	ss := r.URL.Path[19:]

	params := strings.Split(ss, "/")

	if len(params) < 2 {
		http.Error(w, "Invalid params", 500)
		return
	}
	if len(params[0]) == 0 || len(params[0]) > 50 || len(params[1]) == 0 {
		http.Error(w, "Invalid params", 500)
		return
	}
	u, _ := strconv.ParseUint(params[1], 10, 32)
	data, err := db.GetCategoryPosts(params[0], uint(u)*posts_per_page, posts_per_page)
	if err != nil {
		log.Println("Error: getCategoryPostsHandler", err)
		http.Error(w, "getCategoryPostsHandler db err", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(data)

}
func getPubPostsHandler(w http.ResponseWriter, r *http.Request) {
	ss := r.URL.Path[14:]
	params := strings.Split(ss, "/")

	if len(params) < 2 {
		http.Error(w, "Invalid params", 500)
		return
	}
	if len(params[0]) == 0 || len(params[0]) > 16 || len(params[1]) == 0 {
		http.Error(w, "Invalid params", 500)
		return
	}
	u, _ := strconv.ParseUint(params[1], 10, 32)
	data, err := db.GetPubPosts(params[0], uint(u)*posts_per_page, posts_per_page)
	if err != nil {
		log.Println("Error: getCategoryPostsHandler", err)
		http.Error(w, "getCategoryPostsHandler db err", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(data)

}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	ss := r.URL.Path[12:]
	params := strings.SplitN(ss, "/",2)

	if len(params) < 2 {
		http.Error(w, "Invalid params", 500)
		return
	}
	if len(params[0]) == 0 || len(params[1]) == 0 {
		http.Error(w, "Invalid params", 500)
		return
	}

	u, _ := strconv.Atoi(params[0])

	searchStr:=""
	terms := strings.Split(params[1], " ")
	for _, term := range terms {
		//fmt.Println(term)
		if len(term)<1{
			continue
		}
		if term[:1]=="+"{
			if len(term[1:]) == 0 {
				continue
			}
			searchStr+=(" +\""+term[1:]+"\"")
			continue
		}
		if term[:1]=="-"{
			if len(term[1:]) == 0 {
				continue
			}
			searchStr+=(" -\""+term[1:]+"\"")
			continue
		}
		searchStr+=(" +\""+term+"\"")

	}
	//fmt.Println(searchStr)

	ids, err := search.Search(searchStr,posts_per_page,u*posts_per_page)
	if err != nil {
		//log.Println("Error: searchHandler", err)
		http.Error(w, "searchHandler search err", 500)
		return
	}

	data, err := db.GetPostsWithIds(ids)
	if err != nil {
		log.Println("Error: searchHandler", err)
		http.Error(w, "searchHandler db err", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(data)

}

func getPubReplyHandler(w http.ResponseWriter, r *http.Request) {
	ss := r.URL.Path[14:]

	params := strings.Split(ss, "/")

	if len(params) < 2 {
		http.Error(w, "Invalid params", 500)
		return
	}
	if len(params[0]) == 0 || len(params[1]) == 0 {
		http.Error(w, "Invalid params", 500)
		return
	}

	data, err := db.GetPublishersReplyPosts(params[0], params[1])
	if err != nil {
		log.Println("Error: getPubReplyHandler", err)
		http.Error(w, "getPubReplyHandler db err", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(data)

}
func getPublishersHandler(w http.ResponseWriter, r *http.Request) {

	publishers, err := rpc.GetFollowing(adminTwisterUsername)
	if err != nil {
		log.Println("Error: getPublishersHandler can not fetch following users for", adminTwisterUsername, "from Twister RPC server.", err)
		http.Error(w, "getPublishersHandler RPC err", 500)
		return
	}

	data, _ := json.Marshal(publishers)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(data)

}
func regPublisherHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Invalid request method.", 405)
		return
	}
	ss := r.URL.Path[9:]

	if len(ss) < 1 || len(ss) > 16 {
		http.Error(w, "Invalid params", 404)
		return
	}

	result, err := rpc.Follow(adminTwisterUsername, []string{ss})
	if err != nil {
		log.Println("Error: regPublisherHandler can not follow user", result, err)
		http.Error(w, "regPublisherHandler RPC err", 500)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte("{\"ok\" : true}"))

}
func startHttpServer() {
	//http.Handle("/tmpfiles/", http.StripPrefix("/tmpfiles/", http.FileServer(http.Dir("/tmp"))))
	//http.Error(w, http.StatusText(500), 500)
	http.HandleFunc("/api/", handler)
	http.HandleFunc("/api/posts/", getPostsHandler)
	http.HandleFunc("/api/categoryposts/", getCategoryPostsHandler)
	http.HandleFunc("/api/pubposts/", getPubPostsHandler)
	http.HandleFunc("/api/search/", searchHandler)

	http.HandleFunc("/api/pubreply/", getPubReplyHandler)
	http.HandleFunc("/api/publishers/", getPublishersHandler)
	http.HandleFunc("/api/reg/", regPublisherHandler)
	http.HandleFunc("/", homeHandler)
	log.Println("Starting http server...")
	log.Fatal(http.ListenAndServe(":"+config.HttpServerPort, nil))
}
