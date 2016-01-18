package db

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"strconv"
	"time"
)

const max_post_id = 99999999

var db *bolt.DB

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
func Ui64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func Init() {
	var err error
	log.Println("Open DB: acgsh_bolt.db")
	db, err = bolt.Open("acgsh_bolt.db", 0600, &bolt.Options{Timeout: 1 * time.Second})

	if err != nil {
		log.Fatal(err)
	}

	//Initialise all buckets
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("Posts"))
		if err != nil {
			log.Printf("DB create Posts bucket: %s", err)
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("Publishers"))
		if err != nil {
			log.Printf("DB create Publisher bucket: %s", err)
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("PublishersReplyPosts"))
		if err != nil {
			log.Printf("DB create PublishersReplyPosts bucket: %s", err)
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("PublishersPostsIndex"))
		if err != nil {
			log.Printf("DB create PublishersPostsIndex bucket: %s", err)
			return err
		}
		// Category of posts
		_, err = tx.CreateBucketIfNotExists([]byte("CategoryPostsIndex"))
		if err != nil {
			log.Printf("DB create CategoryPostsIndex bucket: %s", err)
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("NameKPostsIndex"))
		if err != nil {
			log.Printf("DB create NameKPostsIndex bucket: %s", err)
			return err
		}
		return nil
	})

	// Check stats
	/*	go func() {
		// Grab the initial stats.
		prev := db.Stats()

		for {
			// Wait for 10s.
			time.Sleep(10 * time.Second)

			// Grab the current stats and diff them.
			stats := db.Stats()
			diff := stats.Sub(&prev)

			// Encode stats to JSON and print to STDERR.
			json.NewEncoder(os.Stderr).Encode(diff)

			// Save stats for the next loop.
			prev = stats
		}
	}()*/

	log.Println("DB initialised successfully.")
}

func Close() {

	db.Close()
	log.Println("DB closed.")

}

func AddPublishersIfNotExist(names *[]string) error {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("Publishers"))
		if bucket == nil {
			return fmt.Errorf("Bucket Publisher not found!")
		}

		for _, name := range *names {
			//fmt.Printf("Value [%d] is [%s]\n", index, name)
			v := bucket.Get([]byte(name))
			if v == nil {
				err := bucket.Put([]byte(name), []byte{})
				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	return err
}

type SyncData struct {
	Max    int64 `json:"max"`
	Latest int64 `json:"latest"`
	Since  int64 `json:"since"`
}

func newSyncData() *SyncData {
	return &SyncData{
		Max:    max_post_id,
		Latest: -1,
		Since:  -1,
	}
}

func GetPublishers() (map[string]SyncData, error) {
	publishers := make(map[string]SyncData)
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("Publishers"))
		if bucket == nil {
			return fmt.Errorf("Bucket Publishers not found!")
		}

		bucket.ForEach(func(k, v []byte) error {
			//fmt.Printf("key=%s, value=%s\n", k, v)
			var sd *SyncData
			sd = newSyncData()
			if len(v) == 0 {
				publishers[string(k)] = *sd
				return nil
			}

			err := json.Unmarshal(v, sd)
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB GetPublishers Unmarshal: %s", v)
				sd = newSyncData()
				publishers[string(k)] = *sd
				return nil
			}

			publishers[string(k)] = *sd
			return nil
		})

		return nil
	})
	return publishers, err
}

func UpdatePublishers(publishers *map[string]SyncData) error {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("Publishers"))
		if bucket == nil {
			return fmt.Errorf("Bucket Publisher not found!")
		}

		for name, sd := range *publishers {
			//fmt.Printf("Value [%d] is [%s]\n", index, name)
			jsonData, err := json.Marshal(sd)
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB UpdatePublishers Marshal: %+v\n", sd)
				continue
			}
			//log.Printf("%s\n", jsonData)

			err = bucket.Put([]byte(name), jsonData)
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB UpdatePublishers bucket.Put: %s : %s\n", name, jsonData)
				continue
			}
		}

		return nil
	})

	return err
}

func DeletePublishers(names *[]string) error {
	err := db.Update(func(tx *bolt.Tx) error {
		/*		bucket, err := tx.CreateBucketIfNotExists("fsef")
				if err != nil {
					return err
				}

				err = bucket.Put(key, value)
				if err != nil {
					return err
				}*/
		return nil
	})

	return err
}

func AddPosts(posts *ShPosts) {
	for _, sp := range *posts {
		_ = db.Update(func(tx *bolt.Tx) error {
			bucketP := tx.Bucket([]byte("Posts"))
			if bucketP == nil {
				return fmt.Errorf("Bucket Posts not found!")
			}
			bucketPPI := tx.Bucket([]byte("PublishersPostsIndex"))
			if bucketPPI == nil {
				return fmt.Errorf("Bucket PublishersPostsIndex not found!")
			}
			bucketCPI := tx.Bucket([]byte("CategoryPostsIndex"))
			if bucketCPI == nil {
				return fmt.Errorf("Bucket CategoryPostsIndex not found!")
			}
			bucketNKPI := tx.Bucket([]byte("NameKPostsIndex"))
			if bucketNKPI == nil {
				return fmt.Errorf("Bucket NameKPostsIndex not found!")
			}

			///////////////////////////////////////
			// Put into Posts bucket
			category := getCategory(sp.Category)
			sp.Category = category
			//fmt.Printf("Value [%d] is [%s]\n", index, name)
			jsonData, err := json.Marshal(sp)
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB addPosts Marshal: %+v\n", sp)
				return err
			}
			//			log.Printf("%s\n", jsonData)

			id := Ui64tob(sp.Time)
			id = append(id, ":"...)
			id = append(id, sp.N...)
			id = append(id, ":"...)
			id = append(id, strconv.FormatInt(sp.K, 10)...)
			err = bucketP.Put(id, jsonData)
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB addPosts bucket.Put: %s : %s\n", id, jsonData)
				return err
			}
			//			fmt.Printf("Value [%s] is [%s]\n", bs, jsonData)

			///////////////////////////////////////
			// Put into NameKPostsIndex bucket
			nk := []byte{}
			nk = append(nk, sp.N...)
			nk = append(nk, ":"...)
			nk = append(nk, strconv.FormatInt(sp.K, 10)...)
			err = bucketNKPI.Put(nk, id)
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB addPosts NameKPostsIndex bucket.Put: %s : %s\n", nk, jsonData)
				return err
			}

			///////////////////////////////////////
			// Put into PublishersPostsIndex bucket
			bucketPP, err := bucketPPI.CreateBucketIfNotExists([]byte(sp.N))
			if err != nil {
				log.Printf("DB CreateBucketIfNotExists PublishersPostsIndex bucket: %s: %s\n", sp.N, err)
				return err
			}
			err = bucketPP.Put(id, []byte{})
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB AddPosts PublishersPostsIndex bucket.Put: %s : %s\n", sp.N, id)
				return err
			}
			///////////////////////////////////////
			// Put into CategoryPostsIndex bucket

			bucketCP, err := bucketCPI.CreateBucketIfNotExists([]byte(category))
			if err != nil {
				log.Printf("DB CreateBucketIfNotExists CategoryPostsIndex bucket: %s: %s\n", category, err)
				return err
			}
			err = bucketCP.Put(id, []byte{})
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB AddPosts CategoryPostsIndex bucket.Put: %s : %s %s\n", sp.Category, category, id)
				return err
			}
			return nil
		})
	}

}

func getCategory(str string) string {
	/*
		動畫
		季度全集
		漫畫
		音樂
		日劇
		ＲＡＷ
		遊戲
		特攝
		其他
	*/
	category := "其他"
	if str == "動畫" || str == "动画" {
		category = "動畫"
	}
	if str == "季度全集" || str == "季度全集" {
		category = "季度全集"
	}
	if str == "漫畫" || str == "漫画" {
		category = "漫畫"
	}
	if str == "音樂" || str == "音乐" || str == "動漫音樂" || str == "动漫音乐" {
		category = "音樂"
	}
	if str == "日劇" || str == "日剧" {
		category = "日劇"
	}
	if str == "ＲＡＷ" || str == "RAW" {
		category = "ＲＡＷ"
	}
	if str == "遊戲" || str == "游戏" {
		category = "遊戲"
	}
	if str == "特攝" || str == "特摄" {
		category = "特攝"
	}

	return category
}

// todo: Check if reply post was reply to acgsh post in DB.
func AddPublishersReplyPosts(posts *ShPubReplyPosts) error {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("PublishersReplyPosts"))
		if bucket == nil {
			return fmt.Errorf("Bucket PublishersReplyPosts not found!")
		}

		for _, sp := range *posts {
			jsonData, err := json.Marshal(sp)
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB AddPublishersReplyPosts Marshal: %+v\n", sp)
				continue
			}
			bs := []byte(sp.N)
			bs = append(bs, ":"...)
			bs = append(bs, strconv.FormatInt(sp.K, 10)...)

			newData := []byte{}
			v := bucket.Get(bs)
			if v == nil {
				newData = append(newData, "["...)
			} else if len(v) >= 2 {
				newData = append(newData, v[:(len(v)-1)]...)
				newData = append(newData, ","...)
			} else {
				newData = append(newData, "["...)
			}
			newData = append(newData, jsonData...)
			newData = append(newData, "]"...)

			err = bucket.Put(bs, newData)
			if err != nil {
				log.Println(err)
				log.Printf("Error: DB AddPublishersReplyPosts bucket.Put: %s : %s\n", bs, newData)
				continue
			}
		}

		return nil
	})

	return err
}

func GetPosts(idx, n uint) ([]byte, error) {
	if n < 1 {
		return []byte{}, fmt.Errorf("Invalid n")
	}
	buf := []byte("[")
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("Posts"))
		if bucket == nil {
			return fmt.Errorf("Bucket Posts not found!")
		}

		cur := bucket.Cursor()

		comma := []byte(",")
		i := uint(0)
		for k, v := cur.Last(); k != nil; k, v = cur.Prev() {
			if i >= idx {
				if i != idx {
					buf = append(buf, comma...)
				}
				buf = append(buf, v...)
			}
			i = i + 1
			if i >= idx+n {
				break
			}
		}

		return nil
	})
	buf = append(buf, "]"...)
	return buf, err
}
func GetPostsWithIds(ids [][]byte) ([]byte, error) {

	if len(ids) == 0 {
		return []byte("[]"), nil
	}

	buf := []byte("[")
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("Posts"))
		if bucket == nil {
			return fmt.Errorf("Bucket Posts not found!")
		}

		comma := []byte(",")

		for _, id := range ids {
			value := bucket.Get(id)
			if value == nil {
				continue
			}
			buf = append(buf, value...)
			buf = append(buf, comma...)
		}

		return nil
	})
	buf = buf[:len(buf)-1]
	buf = append(buf, "]"...)

	return buf, err
}

func GetCategoryPosts(category string, idx, n uint) ([]byte, error) {
	if n < 1 {
		return []byte{}, fmt.Errorf("Invalid n")
	}
	bufIds := [][]byte{}
	err := db.View(func(tx *bolt.Tx) error {
		bucketCPI := tx.Bucket([]byte("CategoryPostsIndex"))
		if bucketCPI == nil {
			return fmt.Errorf("Bucket CategoryPostsIndex not found!")
		}

		bucketCP := bucketCPI.Bucket([]byte(category))
		if bucketCP == nil {
			//return fmt.Errorf("Bucket CategoryPostsIndex %s not found!",category)
			return nil
		}

		cur := bucketCP.Cursor()

		i := uint(0)
		for k, _ := cur.Last(); k != nil; k, _ = cur.Prev() {
			if i >= idx {
				bufIds = append(bufIds, k)
			}
			i = i + 1
			if i >= idx+n {
				break
			}
		}

		return nil
	})
	if err != nil {
		return []byte{}, err
	}

	return GetPostsWithIds(bufIds)

}
func GetPubPosts(publisher string, idx, n uint) ([]byte, error) {
	if n < 1 {
		return []byte{}, fmt.Errorf("Invalid n")
	}
	bufIds := [][]byte{}
	err := db.View(func(tx *bolt.Tx) error {
		bucketPPI := tx.Bucket([]byte("PublishersPostsIndex"))
		if bucketPPI == nil {
			return fmt.Errorf("Bucket PublishersPostsIndex not found!")
		}

		bucketPP := bucketPPI.Bucket([]byte(publisher))
		if bucketPP == nil {
			//return fmt.Errorf("Bucket PublishersPostsIndex %s not found!",publisher)
			return nil
		}

		cur := bucketPP.Cursor()

		i := uint(0)
		for k, _ := cur.Last(); k != nil; k, _ = cur.Prev() {
			if i >= idx {
				bufIds = append(bufIds, k)
			}
			i = i + 1
			if i >= idx+n {
				break
			}
		}

		return nil
	})
	if err != nil {
		return []byte{}, err
	}
	return GetPostsWithIds(bufIds)
}

func GetPublishersReplyPosts(name, k string) ([]byte, error) {
	var data []byte
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("PublishersReplyPosts"))
		if bucket == nil {
			return fmt.Errorf("Bucket PublishersReplyPosts not found!")
		}

		bs := []byte(name)
		bs = append(bs, ":"...)
		bs = append(bs, k...)
		value := bucket.Get(bs)
		if value == nil {
			value = []byte("[]")
		}

		data = make([]byte, len(value))
		copy(data, value)

		return nil
	})

	return data, err
}
