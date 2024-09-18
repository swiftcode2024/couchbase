package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/couchbase/gocb/v2"
)

func main() {
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

	cluster, err := gocb.Connect("couchbases://"+connectionString, options)
	if err != nil {
		log.Fatal(err)
	}

	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Perform a N1QL Query
	defaultScope := bucket.Scope("_default")
	queryResult, err := defaultScope.Query(
		fmt.Sprintf("select id, realW, expectedW from (SELECT meta().id as id , TO_NUMBER(w) as realW, 30 * TO_NUMBER(sis[siNum-1].aw) / TO_NUMBER(sis[siNum-1].tbb) as expectedW FROM `48-main`) as result WHERE realW != expectedW and realW != 0 limit 1;"),
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
