package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
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
	idMap := make(map[int64]int64, 100)
	if len(args) > 4 {
		idStr := strings.Split(args[4], ",")
		for i := 0; i < len(idStr); i++ {
			number, _ := strconv.Atoi(idStr[i])
			idMap[int64(number)] = int64(number)
		}
	}

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

		if len(idMap) > 0 {
			_, ok := idMap[gameId]
			if !ok {
				continue
			}
		}
		log.Printf("检查游戏:%v bbm:%v", gameId, bbm)

		checkContinues(defaultScope, gameId)
		checkWeightEqZero(defaultScope, gameId, bbm)
		checkWeightNeZero(defaultScope, gameId, bbm)
		checkSummary(defaultScope, gameId)

	}
}

func checkContinues(scope *gocb.Scope, gameId int64) {
	queryResult, err := scope.Query(
		fmt.Sprintf(
			"select count(*) from `%v-main` where consistent = false or sis[0].hashr not like '0:%%'", gameId),
		&gocb.QueryOptions{
			Timeout: 10 * time.Minute,
		},
	)
	if err != nil {
		log.Println(err)
		return
	}

	// Print each found Row
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Println(err)
		}
		resultMap := result.(map[string]interface{})
		fmt.Printf("连续性检查返回:%v\n", resultMap["$1"])
	}

	if err := queryResult.Err(); err != nil {
		log.Println(err)
	}
}

func checkWeightEqZero(scope *gocb.Scope, gameId int64, bbm int) {
	queryResult, err := scope.Query(
		fmt.Sprintf(
			"select id, realW, expectedW from (SELECT meta().id as id , TO_NUMBER(w) as realW, %v * TO_NUMBER(sis[siNum-1].aw) / TO_NUMBER(sis[siNum-1].tbb) as expectedW FROM `%v-main`) as result WHERE realW != expectedW and realW = 0 limit 1", bbm, gameId),
		&gocb.QueryOptions{
			Timeout: 10 * time.Minute,
		},
	)
	if err != nil {
		log.Println(err)
		return
	}

	// Print each found Row
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Println(err)
		}
		fmt.Printf("权重检查w=0返回:%v\n", result)
	}

	if err := queryResult.Err(); err != nil {
		log.Println(err)
	}
}

func checkWeightNeZero(scope *gocb.Scope, gameId int64, bbm int) {
	queryResult, err := scope.Query(
		fmt.Sprintf(
			"select id, realW, expectedW from (SELECT meta().id as id , TO_NUMBER(w) as realW, %v * TO_NUMBER(sis[siNum-1].aw) / TO_NUMBER(sis[siNum-1].tbb) as expectedW FROM `%v-main`) as result WHERE realW != expectedW and realW != 0 limit 1", bbm, gameId),
		&gocb.QueryOptions{
			Timeout: 10 * time.Minute,
		},
	)
	if err != nil {
		log.Println(err)
		return
	}

	// Print each found Row
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Println(err)
		}
		fmt.Printf("权重检查w!=0返回:%v\n", result)
	}

	if err := queryResult.Err(); err != nil {
		log.Println(err)
	}
}

func checkSummary(scope *gocb.Scope, gameId int64) {
	queryResult, err := scope.Query(
		fmt.Sprintf(
			"SELECT (SELECT COUNT(*) FROM `%v-main`) AS totalDocs, (SELECT COUNT(*) FROM `%v-main` WHERE consistent=false) AS unConsistentDocs, (SELECT COUNT(*) FROM `%v-main` WHERE w='0') AS noPrizeDocs, (SELECT COUNT(*) FROM `%v-main` WHERE w!='0')  AS prizeDocs, (SELECT COUNT(*) FROM `%v-main` WHERE siNum >= 10)  AS bigPrizeDocs", gameId, gameId, gameId, gameId, gameId),
		&gocb.QueryOptions{
			Timeout: 10 * time.Minute,
		},
	)
	if err != nil {
		log.Println(err)
		return
	}

	// Print each found Row
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Println(err)
		}
		fmt.Printf("综合检查返回:%v\n", result)
	}

	if err := queryResult.Err(); err != nil {
		log.Println(err)
	}
}
