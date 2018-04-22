package main

import (
	"dannytools/ehand"
	"dannytools/logging"
	"dannytools/myredis"

	"fmt"
	"path/filepath"
	"sync"

	"github.com/go-errors/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	// init global variables
	g_logger = logging.NewRawLogger(logging.DEBUG)
	g_conf = &CmdConf{}
}

func main() {

	//parse command options
	g_conf.parseCmdOptions()

	// redis or cluster
	CheckRealRedisType(g_conf)

	//get redis addr to scan keys, prefer latest slaves, otherwise master
	nodesScan := GetNodesAddrToScanKey(g_conf)
	if len(nodesScan) < 1 {
		if g_conf.forceAddr {
			nodesScan = []string{myredis.GetRedisAddr(g_conf.host, int(g_conf.port))}
			logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_WARNING,
				ehand.NAME_MSG: fmt.Sprintf("no suitable slave for scanning keys and -F is set true, use %s to scan keys", nodesScan[0])}, logging.WARNING)
		} else {

			ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf(""), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_ERROR,
				ehand.NAME_MSG: "no suitable redis to scan keys"}, true)
		}
	}

	var (
		keyesChan       chan string = make(chan string, MaxInt(32, int(g_conf.scanThreads)))
		scanThreads     int         = MinInt(int(g_conf.scanThreads), len(nodesScan))
		wgScan          sync.WaitGroup
		wgPrint         sync.WaitGroup
		wgProcess       sync.WaitGroup
		reFile          string                = ""
		printChan       chan myredis.RedisKey = make(chan myredis.RedisKey, MaxInt(32, int(g_conf.processThreads)))
		deletedKeysChan chan string           = make(chan string, MaxInt(64, int(g_conf.processThreads)))
	)

	if g_conf.dryRun {
		// dry run
		logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
			ehand.NAME_MSG: fmt.Sprintf("Dry run, not actually execute action of %s", g_conf.commands)}, logging.INFO)
		keyFile := filepath.Join(g_conf.outdir, gc_dryrun_keys_file)
		//print keys into file
		wgPrint.Add(1)
		go PrintKeysIntoFile(keyFile, keyesChan, &wgPrint)

	} else {
		// actual run

		if g_conf.commands == "dump" || g_conf.commands == "bigkey" {

			if g_conf.commands == "dump" {

				reFile = filepath.Join(g_conf.outdir, gc_result_json_file)
			} else {
				reFile = filepath.Join(g_conf.outdir, gc_result_bigkey_file)
			}

			// print result
			wgPrint.Add(1)
			go PrintKeyInfoAndValueIntoFile(g_conf, reFile, printChan, &wgPrint)

			// process keys
			GetKeyInfoAndValueForDumpOrBigKeys(nodesScan, g_conf, keyesChan, printChan, &wgProcess)

		} else if g_conf.commands == "delete" {

			logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
				ehand.NAME_MSG: "getting suitable redis addr to delete keys"}, logging.INFO)
			masterAddr, err := GetMasterAddr(g_conf)

			ehand.CheckErrNoExtraMsgAlreadyStack(g_logger, err, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REDIS_INFO,
				ehand.NAME_MSG: "fail to get suitable redis addr to delete keys"}, true)

			if masterAddr == "" {
				logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_WARNING,
					ehand.NAME_MSG: fmt.Sprintf("%s:%d is slave and replication is stopped, but -F is set true, connect it to delete keys", g_conf.host, g_conf.port)}, logging.WARNING)
				masterAddr = myredis.GetRedisAddr(g_conf.host, int(g_conf.port))
			}

			logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
				ehand.NAME_MSG: fmt.Sprintf("connect to %s to delete keys", masterAddr)}, logging.INFO)

			// print deleted keys
			keyFile := filepath.Join(g_conf.outdir, gc_deleted_keys_file)
			wgPrint.Add(1)
			go PrintKeysIntoFile(keyFile, deletedKeysChan, &wgPrint)

			// delete keys
			DeleteKeysThreads([]string{masterAddr}, g_conf, keyesChan, deletedKeysChan, &wgProcess)

		} else {
			ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf(""), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_INVALID_OPTION,
				ehand.NAME_MSG: fmt.Sprintf("unsupported command -w=%s", g_conf.commands)}, true)
		}
	}

	// scan keys
	if g_conf.keyfile == "" {
		ScanRedisesKeysThreads(g_conf, nodesScan, scanThreads, keyesChan, &wgScan)
	} else {
		// get keys from file
		wgScan.Add(1)
		go ScanKeysFromFile(g_conf, keyesChan, &wgScan)
	}

	// wait for scan finish and close the channel
	wgScan.Wait()
	close(keyesChan)
	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: "finish scanning keys"}, logging.INFO)

	// wait for process finish and close the channel
	if !g_conf.dryRun {

		wgProcess.Wait()
		if g_conf.commands == "dump" || g_conf.commands == "bigkey" {
			close(printChan)
			logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
				ehand.NAME_MSG: "finish processing keys"}, logging.INFO)
		} else if g_conf.commands == "delete" {
			close(deletedKeysChan)
			logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
				ehand.NAME_MSG: "finish deleting keys"}, logging.INFO)
		} else {
			ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf(""), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_INVALID_OPTION,
				ehand.NAME_MSG: fmt.Sprintf("unsupported command -w=%s", g_conf.commands)}, true)
		}
	}

	// wait for print result finish and close the channel
	wgPrint.Wait()
	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: "finish  writing result"}, logging.INFO)

	logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
		ehand.NAME_MSG: "Complete, Bye!"}, logging.INFO)

}
