package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	_ "modernc.org/sqlite"
)

func predictNextWord(c *gin.Context) {
	input := c.Query("w")
	input = strings.Trim(input, " ")
	db := c.MustGet("ngram_db").(*sql.DB)
	words := strings.Fields(input)
	if len(words) >= 5 {
		words = words[len(words)-4:]
	}
	next := []string{}
	for i := len(words); i > 0; i-- {
		input = strings.Join(words[len(words)-i:], " ")
		rows, err := db.Query(`
	SELECT w2
	FROM ngrams
	WHERE w1 = ?
	ORDER BY count DESC
	LIMIT 3
	`, input)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var nextWord string

			err := rows.Scan(&nextWord)
			if err != nil {
				panic(err)
			}

			next = append(next, nextWord)
		}
		if len(next) > 0 {
			break
		}
	}

	if len(next) != 0 {
		c.IndentedJSON(http.StatusOK, next)
	} else {
		c.IndentedJSON(http.StatusNotFound, "No Prediction")
	}
}

func search(c *gin.Context) {
	input := c.Query("w")
	input = strings.Trim(input, " ")
	db := c.MustGet("lookup_db").(*sql.DB)
	rows, err := db.Query(`
	SELECT row
	FROM data_fts
	WHERE data_fts MATCH ?
	`, input)
	if err != nil {
		panic(err)
	}

	defer rows.Close()
	texts := []string{}
	for rows.Next() {
		var id int
		rows.Scan(&id)
		fmt.Println("ID:", id)
		texts = append(texts, "Text found in Doc"+strconv.Itoa(id))
	}
	if len(texts) != 0 {
		c.IndentedJSON(http.StatusOK, texts)
	} else {
		c.IndentedJSON(http.StatusNotFound, "No Match")
	}
}

func main() {
	db, err := sql.Open("sqlite", "file:../model/ngrams.db?mode=ro")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous=NORMAL;")
	db.Exec("PRAGMA cache_size=100000;")

	router := gin.Default()
	router.Use(func(c *gin.Context) {
		c.Set("ngram_db", db)
		c.Next()
	})
	db, err = sql.Open("sqlite", "file:../model/lookup.db?mode=ro")
	if err != nil {
		panic(err)
	}
	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous=NORMAL;")
	db.Exec("PRAGMA cache_size=100000;")
	router.Use(func(c *gin.Context) {
		c.Set("lookup_db", db)
		c.Next()
	})
	router.GET("/predNxt", predictNextWord)
	router.GET("/search", search)
	router.Run("localhost:8800")

}
