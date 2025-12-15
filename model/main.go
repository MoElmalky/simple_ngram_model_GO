package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strings"

	_ "modernc.org/sqlite"
)

func tokenize(sentence string) []string {
	text := strings.ToLower(sentence)
	re := regexp.MustCompile(`[^\p{L}\p{M}\p{N}\s]+`)
	text = re.ReplaceAllString(text, " ")
	re = regexp.MustCompile(`[\p{M}]+`)
	text = re.ReplaceAllString(text, "")
	return strings.Fields(text)
}

func normalize(s string) string {
	text := strings.ToLower(s)
	re := regexp.MustCompile(`[^\p{L}\p{M}\p{N}\s]+`)
	text = re.ReplaceAllString(text, " ")
	re = regexp.MustCompile(`[\p{M}]+`)
	text = re.ReplaceAllString(text, "")
	return strings.TrimSpace(text)
}

func buildNgram(filePath string, maxGrams int) {

	if maxGrams < 2 {
		maxGrams = 2
	}

	db, err := sql.Open("sqlite", "ngrams.db")
	if err != nil {
		panic(err)
	}

	defer db.Close()

	file, err := os.Open(filePath)

	if err != nil {
		panic(err)
	}

	defer file.Close()
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS ngrams (
    	w1 TEXT NOT NULL,
    	w2 TEXT NOT NULL,
    	count INTEGER NOT NULL,
    	PRIMARY KEY (w1, w2)
		);
	`)
	if err != nil {
		panic(err)
	}
	insertStmt, _ := db.Prepare(`
		INSERT INTO ngrams (w1, w2, count)
		VALUES (?, ?, 1)
		ON CONFLICT(w1, w2)
		DO UPDATE SET count = count + 1;
	`)
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	instertTx := tx.Stmt(insertStmt)
	reader := csv.NewReader(file)

	counter := 0

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		sentence := record[1]
		tokens := tokenize(sentence)
		for i := 1; i < maxGrams; i++ {
			if len(tokens) < i+1 {
				continue
			}
			for j := 0; j < len(tokens)-1; j++ {

				if j+i > len(tokens)-1 {
					break
				}

				w := strings.Join(tokens[j:j+i], " ")

				// w := ""

				// for k := j; k < j+i; k++ {
				// 	w += " "
				// 	w += tokens[k]
				// }

				n := tokens[j+i]
				// if ngrams[w] == nil {
				// 	ngrams[w] = map[string]int{}
				// }

				// ngrams[w][n]++
				_, _ = instertTx.Exec(w, n)
			}
		}
		counter++
		if counter%500 == 0 {
			err := tx.Commit()
			if err != nil {
				panic(err)
			}
			db.Exec("delete from ngrams where count < 2;")
			tx, _ = db.Begin()
			instertTx = tx.Stmt(insertStmt)
			fmt.Println("Rows processed:", counter)
			if counter%10000 == 0 {
				db.Exec("delete from ngrams where count < 5;")
			}
		}
	}
	_ = tx.Commit()
	db.Exec("delete from ngrams where count < 5;")
}

func buildLookup(filePath string) {
	db, err := sql.Open("sqlite", "lookup.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(`
	CREATE VIRTUAL TABLE IF NOT EXISTS data_fts
	USING fts5(text, row UNINDEXED);
	`)
	if err != nil {
		panic(err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	insertStmt, err := db.Prepare(`INSERT INTO data_fts(text, row) VALUES (?,?)`)
	if err != nil {
		panic(err)
	}
	defer insertStmt.Close()

	counter := 0
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		text := strings.TrimSpace(record[1])
		text = normalize(text)

		_, err = insertStmt.Exec(text, counter+1)
		if err != nil {
			panic(err)
		}
		counter++
		if counter%10 == 0 {
			fmt.Println("Rows processed:", counter)
			if counter%500 == 0 {
				break
			}
		}
	}

	fmt.Println("Lookup table built successfully.")
}

func main() {

	// sentences := []string{
	// 	"I love to code in go",
	// 	"I like to eat pizza every day",
	// 	"I want to eat something fresh",
	// 	"I love walking",
	// 	"you are my best friend",
	// 	"we should go hiking this weekend",
	// 	"the weather is nice today",
	// 	"let's grab some coffee",
	// 	"reading books is fun",
	// 	"traveling the world is my dream"}

	//buildNgram("arabic_sentiment_reviews.csv", 5)
	buildLookup("arabic_sentiment_reviews.csv")
}
