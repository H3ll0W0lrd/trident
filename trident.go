package main

import (
	"bufio"
	"dannytools/ehand"
	"dannytools/logging"
	"io"
	"math/rand"

	"sort"
	"time"

	"os"
	"strings"
	"sync"

	"github.com/go-errors/errors"

	"dannytools/myredis"
	"fmt"

	"github.com/sirupsen/logrus"
)

func GetConfRedis(cfg *CmdConf, db int) myredis.ConfRedis {
	return myredis.ConfRedis{Addr: fmt.Sprintf("%s:%d", cfg.host, cfg.port), Database: db}
}

func GetConfCluster(cfg *CmdConf, readOnly bool) myredis.ConfCluster {
	return myredis.ConfCluster{Addrs: []string{fmt.Sprintf("%s:%d", cfg.host, cfg.port)},
		ReadOnly: readOnly,
	}
}

func CreateRedisOrClusterClient(cfg *CmdConf, readOnly bool, db int) (myredis.ClusterAndRedisClient, error) {
	var (
		client myredis.ClusterAndRedisClient = myredis.ClusterAndRedisClient{IsCluster: cfg.isCluster}
		err    error
	)
	if cfg.isCluster {
		cfgCluster := GetConfCluster(cfg, readOnly)
		client.Cluster, err = cfgCluster.CreateNewClientCluster()

	} else {
		cfgRedis := GetConfRedis(cfg, db)
		client.Redis, err = cfgRedis.CreateNewClientRedis()
	}

	return client, err
}

func CheckRealRedisType(cfg *CmdConf) {

	var tp string

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: fmt.Sprintf("check %s:%d is redis or cluster", cfg.host, cfg.port)}, logging.INFO)
	cfg.isCluster = false
	client, err := CreateRedisOrClusterClient(cfg, false, 0)
	if err != nil {
		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err,
			logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_CONNECT,
				ehand.NAME_MSG: fmt.Sprintf("fail to connect to redis %s:%d", cfg.host, cfg.port)}, true)
	}

	ok, err := client.CheckIfCluster()
	if err != nil {
		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err,
			logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_INFO,
				ehand.NAME_MSG: fmt.Sprintf("fail to get redis info %s:%d", cfg.host, cfg.port)}, true)
	}

	if ok {
		cfg.isCluster = true
		tp = "cluster"
	} else {
		cfg.isCluster = false
		tp = "redis"
	}

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: fmt.Sprintf("%s:%d is %s", cfg.host, cfg.port, tp)}, logging.INFO)

}

/*
get redis master addr. if it is cluster, just return the addr specified on comman line.
if it is redis and it is master,  return the addr specified on comman line, otherwise, get master addr of it
*/
func GetMasterAddr(cfg *CmdConf) (string, error) {
	var (
		err    error
		client myredis.ClusterAndRedisClient
	)
	if cfg.isCluster {
		return myredis.GetRedisAddr(cfg.host, int(cfg.port)), nil
	} else {
		client, err = CreateRedisOrClusterClient(cfg, false, 0)
		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_CONNECT,
			ehand.NAME_MSG: fmt.Sprintf("fail to connect to %s:%d", cfg.host, cfg.port)}, true)
		roleInfo, err := client.GetRedisInfoRole()
		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_INFO,
			ehand.NAME_MSG: fmt.Sprintf("fail to get redis info %s:%d", cfg.host, cfg.port)}, true)
		if roleInfo.Role == "master" {
			return myredis.GetRedisAddr(cfg.host, int(cfg.port)), nil
		} else if roleInfo.Replicating {
			return myredis.GetRedisAddr(roleInfo.MasterAddr.Host, roleInfo.MasterAddr.Port), nil
		} else {
			// it is slave, but replication is stopped
			if cfg.forceAddr {
				return "", nil
			} else {
				return "", ehand.WithStackError(fmt.Errorf("%s:%d is slave, but replication is stopped", cfg.host, cfg.port))
			}
		}
	}
}

/*
	cluster: get latest slave of each master, if not found, use master
	redis: get latest slave, if not found, use master
*/
func GetNodesAddrToScanKey(cfg *CmdConf) []string {
	var (
		err        error
		client     myredis.ClusterAndRedisClient
		allMasters []string
	)
	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK, ehand.NAME_MSG: "start to get latest slaves to scan keys"}, logging.INFO)
	client, err = CreateRedisOrClusterClient(cfg, false, 0)
	ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_CONNECT,
		ehand.NAME_MSG: fmt.Sprintf("fail to connect to %s:%d", cfg.host, cfg.port)}, true)
	if cfg.isCluster {

		nodesInfo, err := myredis.GetClusterNodesInfo(client.Cluster)

		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_CLUSTER_NODES,
			ehand.NAME_MSG: fmt.Sprintf("fail to get cluster nodes info %s:%d", cfg.host, cfg.port)}, true)

		allMasters = myredis.GetAllMastersAddr(nodesInfo)
		client.Cluster.Close()

	} else {

		roleInfo, err := client.GetRedisInfoRole()
		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_INFO,
			ehand.NAME_MSG: fmt.Sprintf("fail to get redis info %s:%d", cfg.host, cfg.port)}, true)
		if roleInfo.Role == "master" {
			allMasters = append(allMasters, myredis.GetRedisAddr(cfg.host, int(cfg.port)))
		} else {
			if !roleInfo.Replicating {
				if cfg.forceAddr {
					logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_WARNING,
						ehand.NAME_MSG: fmt.Sprintf("%s:%d is a slave, but replication is stopped", cfg.host, cfg.port)}, logging.WARNING)
					return []string{}
				} else {
					ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf(""),
						logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_REPLICATE_STOP,
							ehand.NAME_MSG: fmt.Sprintf("%s:%d is a slave, but replication is stopped", cfg.host, cfg.port)}, true)
				}

			}
			allMasters = append(allMasters, myredis.GetRedisAddr(roleInfo.MasterAddr.Host, roleInfo.MasterAddr.Port))
		}
		client.Redis.Close()
	}

	// get latest slaves
	_, slaves, masters, err := myredis.GetLatestSlaveOfMasters(allMasters)
	ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_INFO,
		ehand.NAME_MSG: "fail to get latest slaves to scan keys"}, true)
	if len(masters) > 0 {
		logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
			ehand.NAME_MSG: fmt.Sprintf("below redises have no online slave, use them to scan keys:\n\t%s\n", strings.Join(masters, " "))}, logging.WARNING)
		slaves = append(slaves, masters...)
	}
	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: fmt.Sprintf("use below redises to scan keys:\n\t%s\n", strings.Join(slaves, " "))}, logging.INFO)

	return slaves
}

/*
set up threads to scan redis keys
*/

func ScanRedisesKeysThreads(cfg *CmdConf, redisAddrs []string, scanThreads int, keyesChan chan string, wg *sync.WaitGroup) {
	var (
		nodesCnt int         = len(redisAddrs)
		addrChan chan string = make(chan string, nodesCnt)

		i int
	)

	for _, addr := range redisAddrs {
		addrChan <- addr
	}

	for i = 1; i <= scanThreads; i++ {
		wg.Add(1)
		go ScanOneRedisOneThread(i, cfg, addrChan, keyesChan, wg)
	}

	close(addrChan)

}

/*
	scan one redis
*/
func ScanOneRedisOneThread(i int, cfg *CmdConf, addrChan chan string, keyesChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: fmt.Sprintf("start thread %d of scanning key", i)}, logging.INFO)
	for addr := range addrChan {
		logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
			ehand.NAME_MSG: fmt.Sprintf("connect to %s to scan keys", addr)}, logging.INFO)

		redisCfg := myredis.ConfRedis{Addr: addr, Database: int(cfg.database)}
		redisClient, err := redisCfg.CreateNewClientRedis()
		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_CONNECT,
			ehand.NAME_MSG: "fail to connect to " + addr}, true)

		err = myredis.ScanRedisKeys(redisClient, gc_scankeys_batch, cfg.patternReg, int(cfg.keyBatch), int(cfg.keyInterval), keyesChan)
		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_SCAN,
			ehand.NAME_MSG: "fail to scan keys of " + addr}, true)

		logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
			ehand.NAME_MSG: fmt.Sprintf("finish scanning keys of %s", addr)}, logging.INFO)
		redisClient.Close()
	}
	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: fmt.Sprintf("exit thread %d of scanning key", i)}, logging.INFO)

}

/*
	get key info and values fot dump or finding big key, one thread
*/

func GetKeyInfoAndValueOneThread(i uint, client myredis.ClusterAndRedisClient, cfg *CmdConf, ifDump bool, keyesChan chan string, printChan chan myredis.RedisKey, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		oneVal      myredis.RedisKey
		err         error
		ifCountMem  bool = true   // always count the memory
		ifNeedValue bool = ifDump // for finding big key, we don't need value

		keySleepDuration     time.Duration = time.Duration(cfg.keyInterval) * time.Microsecond
		elementBatch         int           = int(cfg.elementBatch)
		elementSleepDuration time.Duration = time.Duration(cfg.elementInterval) * time.Microsecond
		//str                  string
		processedCnt   uint = 0
		ifElementSleep bool

		sizeLimit int = int(cfg.sizeLimit)
	)
	if cfg.elementInterval > 0 {
		ifElementSleep = true
	}

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: fmt.Sprintf("start thread %d of processing key", i)}, logging.INFO)
	for key := range keyesChan {
		oneVal, err = client.GetKeyValue(key, gc_scanelement_batch, elementBatch, elementSleepDuration, ifElementSleep, ifCountMem, ifNeedValue)

		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_GET_KEY_VALUE,
			ehand.NAME_MSG: "fail to get value of key " + key}, true)

		if oneVal.Type == "none" {
			// key not exists
			continue
		} else if oneVal.Expire == "-2" {
			// key expired
			continue
		} else if oneVal.Expire == "-1" {
			oneVal.Expire = ""
		}
		// only care keys whose size is equal or bigger than sizeLimit
		if oneVal.Bytes < sizeLimit {
			continue
		}
		oneVal.Database = cfg.database
		printChan <- oneVal

		if cfg.keyInterval > 0 {
			processedCnt++
			if processedCnt > cfg.keyBatch {
				processedCnt = 0
				time.Sleep(keySleepDuration)
			}
		}

	}

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: fmt.Sprintf("exit thread %d of processing key", i)}, logging.INFO)

}

/*
get key info and values fot dump or finding big key, multi threads
*/
func GetKeyInfoAndValueForDumpOrBigKeys(addrs []string, cfg *CmdConf, keyesChan chan string, printChan chan myredis.RedisKey, wg *sync.WaitGroup) {
	var (
		err    error
		i      uint                          = 0
		client myredis.ClusterAndRedisClient //= myredis.ClusterAndRedisClient{IsCluster: cfg.isCluster}
		ifDump bool                          = false
	)

	if cfg.commands == "dump" {
		ifDump = true
	}

	// only support db 0
	client, err = myredis.GetClusterOrRedisClient(myredis.ConfCommon{PoolSize: int(cfg.processThreads) + 1}, addrs, true, int(cfg.database), cfg.isCluster)
	ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_CONNECT,
		ehand.NAME_MSG: "fail to connect to redis " + addrs[0]}, true)

	for i = 1; i <= cfg.processThreads; i++ {
		wg.Add(1)
		go GetKeyInfoAndValueOneThread(i, client, cfg, ifDump, keyesChan, printChan, wg)
	}

}

/*
 delete keys by expire them
*/

func DeleteKeysThreads(masterAddrs []string, cfg *CmdConf, keysChan chan string, deletedKeysChan chan string, wg *sync.WaitGroup) {
	var (
		i      uint = 0
		err    error
		client myredis.ClusterAndRedisClient
	)

	client, err = myredis.GetClusterOrRedisClient(myredis.ConfCommon{PoolSize: int(cfg.processThreads) + 1}, masterAddrs, false, int(cfg.database), cfg.isCluster)
	ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_CONNECT,
		ehand.NAME_MSG: "fail to connect to redis " + masterAddrs[0]}, true)

	for i = 1; i <= cfg.processThreads; i++ {
		wg.Add(1)
		go DeleteKeysByExpireThemThread(i, client, cfg, keysChan, deletedKeysChan, wg)
	}
}

/*
 delete keys thread
*/
func DeleteKeysByExpireThemThread(i uint, client myredis.ClusterAndRedisClient, cfg *CmdConf, keysChan chan string, deletedKeysChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		err           error
		alreadyCnt    uint          = 0
		expire        int           = 1
		expMax        int           = int(cfg.delKeyExpireMax)
		sleepDuration time.Duration = time.Duration(cfg.keyInterval) * time.Microsecond
	)

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: fmt.Sprintf("start thread %d of delete key", i)}, logging.INFO)

	for key := range keysChan {
		// set expire time to [1, gc_delete_expire_seconds_max] seconds
		expire = rand.Intn(expMax) + 1
		if cfg.ifDirectDel {
			err = client.DeleteKeysByDirectlyDelIt([]string{key})
		} else {

			err = client.DeleteKeysByExpireIt([]string{key}, time.Duration(expire)*time.Second)
		}
		ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_EXPIRE,
			ehand.NAME_MSG: fmt.Sprintf("fail to delet key %s by set its expire time to %d seconds", key, expire)}, true)

		// record what keys we deleted
		deletedKeysChan <- key

		if cfg.keyInterval > 0 {
			alreadyCnt++
			if alreadyCnt >= cfg.keyBatch {
				alreadyCnt = 0
				time.Sleep(sleepDuration)
			}
		}

	}

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: fmt.Sprintf("exit thread %d of delete key", i)}, logging.INFO)
}

/*
write keys to file, one key one line
*/
func PrintKeysIntoFile(keyFile string, keysChan chan string, wg *sync.WaitGroup) {
	//wg.Add(1)
	defer wg.Done()

	var (
		err error
	)

	fh, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0744)
	if err != nil {
		ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_OPEN,
			ehand.NAME_MSG: fmt.Sprintf("fail to open file %s to write", keyFile)}, true)
	}
	defer fh.Close()

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: "start thread of write keys to file"}, logging.INFO)
	for key := range keysChan {
		_, err = fh.WriteString(key + "\n")
		if err != nil {
			ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_WRITE,
				ehand.NAME_MSG: fmt.Sprintf("fail to write to file %s: %s", keyFile, key)}, true)
		}
	}

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: "exit thread of write keys to file"}, logging.INFO)

}

/*
dump key value or big key info into file
*/

func PrintKeyInfoAndValueIntoFile(cfg *CmdConf, reFile string, printChan chan myredis.RedisKey, wg *sync.WaitGroup) {
	//wg.Add(1)
	defer wg.Done()

	var (
		err           error
		str           string
		oneVal        myredis.RedisKey
		ifBigKey      bool = false
		needSortKeyes []myredis.RedisKey
	)
	if cfg.commands == "bigkey" {
		ifBigKey = true
	}
	fh, err := os.OpenFile(reFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0744)
	if err != nil {
		ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_OPEN,
			ehand.NAME_MSG: fmt.Sprintf("fail to open file %s to write", reFile)}, true)
	}
	defer fh.Close()

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: "start thread of result writing"}, logging.INFO)

	if ifBigKey {
		fh.WriteString(myredis.GetRedisBigkeyHeader())
	}

	if ifBigKey && cfg.ifSortedKey {
		//bigkey with sorting
		for oneVal = range printChan {
			needSortKeyes = append(needSortKeyes, oneVal)
		}
		sort.SliceStable(needSortKeyes, func(i, j int) bool { return needSortKeyes[i].Bytes > needSortKeyes[j].Bytes })
		var alreadyPrintCnt uint = 0
		for _, oneVal = range needSortKeyes {

			str, err = oneVal.GetRedisKeyPrintLine(ifBigKey, cfg.pretty, g_json_indent)
			ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_JSON_MARSHAL,
				ehand.NAME_MSG: fmt.Sprintf("fail to marshal key %s into json string", oneVal.Key)}, true)

			_, err = fh.WriteString(str)

			if err != nil {
				ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_WRITE,
					ehand.NAME_MSG: fmt.Sprintf("fail to write to file %s: %s", reFile, str)}, true)
			}
			alreadyPrintCnt++
			if cfg.limit > 0 && alreadyPrintCnt >= cfg.limit {
				break
			}

		}

	} else {
		//dump key or bigkey without sorting
		for oneVal = range printChan {
			str, err = oneVal.GetRedisKeyPrintLine(ifBigKey, cfg.pretty, g_json_indent)
			ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_JSON_MARSHAL,
				ehand.NAME_MSG: fmt.Sprintf("fail to marshal key %s into json string", oneVal.Key)}, true)

			// for pretty dump
			if !ifBigKey && cfg.pretty {
				_, err = fh.WriteString(gc_json_line_seperator)
				if err != nil {
					ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_WRITE,
						ehand.NAME_MSG: fmt.Sprintf("fail to write to file %s: %s", reFile, gc_json_line_seperator)}, true)
				}

			}

			if !ifBigKey {
				str += "\n"
			}
			_, err = fh.WriteString(str)

			if err != nil {
				ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_WRITE,
					ehand.NAME_MSG: fmt.Sprintf("fail to write to file %s: %s", reFile, str)}, true)
			}

		}
	}

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: "exit thread of result writing"}, logging.INFO)

}

/*
read keys from file, one key one line
*/

func ScanKeysFromFile(cfg *CmdConf, keysChan chan string, wg *sync.WaitGroup) {
	//wg.Add(1)
	defer wg.Done()

	var (
		ifReg bool = false
		line  string
		err   error
	)

	if cfg.patternReg.String() != "" {
		ifReg = true
	}

	FH, err := os.Open(cfg.keyfile)
	if err != nil {
		ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_OPEN,
			ehand.NAME_MSG: fmt.Sprintf("fail to open file %s", cfg.keyfile)}, true)
	}
	defer FH.Close()
	bufFH := bufio.NewReader(FH)

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: "start thread of scanning keys from file " + cfg.keyfile}, logging.INFO)

	for {
		line, err = bufFH.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_READ,
					ehand.NAME_MSG: fmt.Sprintf("fail to read file %s", cfg.keyfile)}, true)
			}
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if ifReg {
			if cfg.patternReg.MatchString(line) {
				keysChan <- line
			}
		} else {
			keysChan <- line
		}

	}

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: "exit thread of scanning keys from file " + cfg.keyfile}, logging.INFO)
}
