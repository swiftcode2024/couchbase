package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"cryptware.lk/couchbase/game"
	"github.com/couchbase/gocb/v2"
)

func main() {
	//gocb.SetLogger(gocb.VerboseStdioLogger())
	args := os.Args[1:]

	connectionString := args[0]
	bucketName := args[1]
	username := args[2]
	password := args[3]

	fmt.Printf("连接:%v %v %v %v\n", connectionString, bucketName, username, password)

	options := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	}

	if err := options.ApplyProfile(gocb.ClusterConfigProfileWanDevelopment); err != nil {
		log.Fatal(err)
	}

	cluster, err := gocb.Connect(connectionString, options)
	if err != nil {
		log.Fatalf("连接失败:%v", err)
	}

	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Perform a N1QL Query
	defaultScope := bucket.Scope("_default")

	for i := 0; i < len(game.GameList); i++ {

		gameId := game.GameList[i].GameId
		bbm := game.GameList[i].Bbm
		log.Printf("检查游戏:%v bbm:%v", gameId, bbm)

		queryResult, err := defaultScope.Query(
			fmt.Sprintf(
				"select id, realW, expectedW from (SELECT meta().id as id , TO_NUMBER(w) as realW, %v * TO_NUMBER(sis[siNum-1].aw) / TO_NUMBER(sis[siNum-1].tbb) as expectedW FROM `%v-main`) as result WHERE realW != expectedW and realW = 0 limit 1", bbm, gameId),
			&gocb.QueryOptions{},
		)
		if err != nil {
			log.Fatal(err)
		}

		// Print each found Row
		for queryResult.Next() {
			var result interface{}
			err := queryResult.Row(&result)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(result)
		}

		if err := queryResult.Err(); err != nil {
			log.Fatal(err)
		}

	}
}
