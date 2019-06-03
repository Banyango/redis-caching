package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/labstack/gommon/log"
	"github.com/labstack/gommon/random"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Server struct {
	Db          *sql.DB
	RedisClient *redis.Client
}

func (s *Server) handleLink() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if keys, ok := r.URL.Query()["id"]; ok {
				id, err := strconv.ParseInt(keys[0], 10, 32)

				if err != nil {
					log.Fatal(err)
				}

				exists := s.RedisClient.Exists(string(id))

				if exists.Val() == 1 {
					io.WriteString(w, "Found in cache" + s.RedisClient.String())
				} else if exists.Val() == 0 {

					query := "select * from links where link.id =" + string(id)

					rows, e := s.Db.Query(query)

					if rows != nil {

						defer rows.Close()

						var result struct {
							id   int
							link string
						}

						for rows.Next() {
							err := rows.Scan(&result.id, &result.link)

							if err != nil {
								log.Error(err)
							}
						}

						s.RedisClient.Set(string(id), result.link, time.Minute * 15)

						if e != nil {
							log.Error(e)
						}

						io.WriteString(w, "Reloaded from DB : " + result.link)

					} else {

						value := random.String(12, random.Alphanumeric)

						tx, e := s.Db.Begin()

						if e != nil {
							log.Fatal(e)
						}

						stmt, e := tx.Prepare("insert into links (id, link) values (?,?)")

						if e != nil {
							log.Fatal(e)
						}

						_, e = stmt.Exec(id, value)

						if e != nil {
							log.Fatal(e)
						}

						e = tx.Commit()

						if e != nil {
							log.Fatal(e)
						}

						s.RedisClient.Set(string(id), value, time.Minute * 15)

						io.WriteString(w, "Created value " + value)
					}

				} else {
					log.Error("Redis error")
				}


			} else {
				http.Error(w, "Incorrect Usage", http.StatusBadRequest)
			}
		}
	}
}

func (s *Server) handleBack() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if keys, ok := r.URL.Query()["id"]; ok {
				id, err := strconv.ParseInt(keys[0], 10, 32)

				if err != nil {
					log.Fatal(err)
				}

				exists := s.RedisClient.Exists(string(id))

				if exists.Val() == 1 {
					io.WriteString(w, "Found in cache" + s.RedisClient.String())
				} else if exists.Val() == 0 {

					query := "select * from links where link.id =" + string(id)

					rows, e := s.Db.Query(query)

					if rows != nil {

						defer rows.Close()

						var result struct {
							id   int
							link string
						}

						for rows.Next() {
							err := rows.Scan(&result.id, &result.link)

							if err != nil {
								log.Error(err)
							}
						}

						s.RedisClient.Set(string(id), result.link, time.Minute * 15)

						if e != nil {
							log.Error(e)
						}

						io.WriteString(w, "Reloaded from DB : " + result.link)

					} else {

						value := random.String(12, random.Alphanumeric)

						s.RedisClient.Set(string(id), value, time.Minute * 15)

						log.Info(fmt.Sprintf("Created cache value: ", value))

						data := map[string]string{
							"id":string(id),
							"data":value,
						}

						bytes, _ := json.Marshal(data)

						s.RedisClient.Publish("cacheChannel", bytes)

						io.WriteString(w, "Created cache value " + value)
					}

				} else {
					log.Error("Redis error")
				}

			} else {
				http.Error(w, "Incorrect Usage", http.StatusBadRequest)
			}
		}
	}
}


func (s *Server) createRoutes() {
	http.HandleFunc("/writeThru", s.handleLink())
	http.HandleFunc("/writeBack", s.handleBack())
}

func (s *Server) ChannelSubscriber() {
	pubsub := s.RedisClient.Subscribe("cacheChannel")

	_, err := pubsub.Receive()
	if err != nil {
		panic(err)
	}

	ch := pubsub.Channel()

	defer pubsub.Close()

	for true {
		select {
			case msg := <-ch:

				data := map[string]string{}

				e := json.Unmarshal([]byte(msg.Payload), &data)

				if e != nil {
					log.Fatal(e)
				}

				tx, e := s.Db.Begin()

				if e != nil {
					log.Fatal(e)
				}
				stmt, e := tx.Prepare("insert into links (id, link) values (?,?)")

				if e != nil {
					log.Fatal(e)
				}

				number, e := strconv.ParseInt(data["id"], 10, 64)

				_, e = stmt.Exec(number, data["data"])

				if e != nil {
					log.Fatal(e)
				}

				e = tx.Commit()

				log.Info(fmt.Sprint("Wrote link id: ", msg.Payload, " val: ", data["data"]))
		}
	}

}

func (s *Server) Start() {

	s.createRoutes()

	go s.ChannelSubscriber()

	http.ListenAndServe(":2001", nil)
}

func main() {

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	os.Remove("./redis.db")

	db, err := sql.Open("sqlite3", "./redis.db")

	failOnError(err)

	defer db.Close()

	init := `
			create table links(id integer not null primary key, link text);
			delete from links;
		`

	_, err = db.Exec(init)

	failOnError(err)

	server := Server{Db:db, RedisClient:client}

	server.Start()

}

func failOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}