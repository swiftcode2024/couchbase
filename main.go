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

		err := checkFlowContinues(defaultScope, gameId)
		if err != nil {
			continue
		}
		err = checkNodeContinues(defaultScope, gameId)
		if err != nil {
			continue
		}
		err = checkWeightEqZero(defaultScope, gameId, bbm)
		if err != nil {
			continue
		}
		err = checkWeightNeZero(defaultScope, gameId, bbm)
		if err != nil {
			continue
		}
		err = checkSummary(defaultScope, gameId)
		if err != nil {
			continue
		}

	}
}

func checkFlowContinues(scope *gocb.Scope, gameId int64) error {
	queryResult, err := scope.Query(
		fmt.Sprintf(
			"select count(*) from `%v-main` where consistent = false or sis[0].hashr not like '0:%%'", gameId),
		&gocb.QueryOptions{
			Timeout: 10 * time.Minute,
		},
	)
	if err != nil {
		log.Println(err)
		return err
	}

	// Print each found Row
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Println(err)
			return err
		}
		// map[$1:0]
		resultMap := result.(map[string]interface{})
		fmt.Printf("流异常检查返回->%v\n", resultMap["$1"])
	}

	if err := queryResult.Err(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func checkNodeContinues(scope *gocb.Scope, gameId int64) error {
	queryResult, err := scope.Query(
		fmt.Sprintf(
			"SELECT META(d).id,  * FROM `%v-main` AS d UNNEST d.sis AS e LET sid_array = ARRAY v.sid FOR v IN d.sis WHEN v.sid IS NOT MISSING END WHERE ARRAY_LENGTH(d) != d.siNum OR ANY idx IN sid_array SATISFIES TO_NUMBER(SUBSTR(e.hashr, 0, POSITION(e.hashr, \":\"))) != ARRAY_POSITION(sid_array, e.sid) END limit 1;", gameId),
		&gocb.QueryOptions{
			Timeout: 10 * time.Minute,
		},
	)
	if err != nil {
		log.Println(err)
		return err
	}

	// Print each found Row
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Println(err)
			return err
		}
		// map[$1:0]
		resultMap := result.(map[string]interface{})
		fmt.Printf("节点顺序检查返回->%v\n", resultMap["id"])
	}

	if err := queryResult.Err(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func checkWeightEqZero(scope *gocb.Scope, gameId int64, bbm int) error {
	queryResult, err := scope.Query(
		fmt.Sprintf(
			"select id, realW, expectedW from (SELECT meta().id as id , TO_NUMBER(w) as realW, %v * TO_NUMBER(sis[siNum-1].aw) / TO_NUMBER(sis[siNum-1].tbb) as expectedW FROM `%v-main`) as result WHERE realW != expectedW and realW = 0 limit 1", bbm, gameId),
		&gocb.QueryOptions{
			Timeout: 10 * time.Minute,
		},
	)
	if err != nil {
		log.Println(err)
		return err
	}

	// Print each found Row
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Println(err)
			return err
		}
		// map[expectedW:5 id:1719672873856#-1614417189 realW:0]
		resultMap := result.(map[string]interface{})
		fmt.Printf("权重检查w=0返回->expectedW:%v realW:%v id:%v\n",
			resultMap["expectedW"], resultMap["realW"], resultMap["id"])
	}

	if err := queryResult.Err(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func checkWeightNeZero(scope *gocb.Scope, gameId int64, bbm int) error {
	queryResult, err := scope.Query(
		fmt.Sprintf(
			"select id, realW, expectedW from (SELECT meta().id as id , TO_NUMBER(w) as realW, %v * TO_NUMBER(sis[siNum-1].aw) / TO_NUMBER(sis[siNum-1].tbb) as expectedW FROM `%v-main`) as result WHERE realW != expectedW and realW != 0 limit 1", bbm, gameId),
		&gocb.QueryOptions{
			Timeout: 10 * time.Minute,
		},
	)
	if err != nil {
		log.Println(err)
		return err
	}

	// Print each found Row
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Println(err)
			return err
		}
		// map[expectedW:5 id:1719672873856#-1614417189 realW:0]
		resultMap := result.(map[string]interface{})
		fmt.Printf("权重检查w!=0返回->expectedW:%v realW:%v id:%v\n",
			resultMap["expectedW"], resultMap["realW"], resultMap["id"])
	}

	if err := queryResult.Err(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func checkSummary(scope *gocb.Scope, gameId int64) error {
	queryResult, err := scope.Query(
		fmt.Sprintf(
			"SELECT (SELECT COUNT(*) FROM `%v-main`) AS totalDocs, (SELECT COUNT(*) FROM `%v-main` WHERE consistent=false) AS unConsistentDocs, (SELECT COUNT(*) FROM `%v-main` WHERE w='0') AS noPrizeDocs, (SELECT COUNT(*) FROM `%v-main` WHERE w!='0')  AS prizeDocs, (SELECT COUNT(*) FROM `%v-main` WHERE siNum >= 10)  AS bigPrizeDocs", gameId, gameId, gameId, gameId, gameId),
		&gocb.QueryOptions{
			Timeout: 10 * time.Minute,
		},
	)
	if err != nil {
		log.Println(err)
		return err
	}

	// Print each found Row
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Println(err)
			return err
		}
		//map[bigPrizeDocs:[map[$1:0]] noPrizeDocs:[map[$1:1.379856e+06]] prizeDocs:[map[$1:412774]] totalDocs:[map[$1:1.79263e+06]] unConsistentDocs:[map[$1:0]]]
		resultMap := result.(map[string]interface{})
		bigPrizeDocs := resultMap["bigPrizeDocs"].([]interface{})[0].(map[string]interface{})
		noPrizeDocs := resultMap["noPrizeDocs"].([]interface{})[0].(map[string]interface{})
		prizeDocs := resultMap["prizeDocs"].([]interface{})[0].(map[string]interface{})
		totalDocs := resultMap["totalDocs"].([]interface{})[0].(map[string]interface{})
		unConsistentDocs := resultMap["unConsistentDocs"].([]interface{})[0].(map[string]interface{})
		fmt.Printf("综合检查返回->bigPrizeDocs:%v noPrizeDocs:%v prizeDocs:%v totalDocs:%v unConsistentDocs:%v \n",
			bigPrizeDocs["$1"], noPrizeDocs["$1"], prizeDocs["$1"], totalDocs["$1"], unConsistentDocs["$1"])
	}

	if err := queryResult.Err(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}
