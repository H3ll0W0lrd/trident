package main

import (
	"dannytools/constvar"
	"dannytools/ehand"

	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/go-errors/errors"
	"github.com/sirupsen/logrus"
	kitsFile "github.com/toolkits/file"
	kitsSlice "github.com/toolkits/slice"
)

const (
	gc_version       string = "trident V1.02 --By danny.lai@vipshop.com | laijunshou@gmail.com"
	gc_usage_comment string = "safely dump keys, delete keys or find bigkeys in redis or redis cluster"

	gc_scankeys_batch            int64 = 100
	gc_scanelement_batch         int64 = 200
	gc_delete_expire_seconds_max uint  = 60

	gc_json_line_seperator = "\n#one key#\n"

	gc_result_json_file   = "redis_dump.json"
	gc_result_bigkey_file = "redis_bigkeys.csv"
	gc_dryrun_keys_file   = "redis_target_keys.txt"
	gc_deleted_keys_file  = "deleted_keys.txt"
)

var (
	g_json_indent string = constvar.JSON_INDENT_TAB

	g_validCmds []string = []string{"dump", "delete", "bigkey"}

	g_logger *logrus.Logger

	g_conf *CmdConf

	g_default_uints map[string]uint = map[string]uint{
		"limit": 100,
	}
)

type CmdConf struct {
	host string
	port uint

	forceAddr bool

	passwd   string
	database uint

	isCluster bool

	commands   string
	pattern    string
	patternReg *regexp.Regexp
	keyfile    string
	dryRun     bool

	outdir string

	keyBatch        uint
	keyInterval     uint
	elementBatch    uint
	elementInterval uint

	processThreads uint
	scanThreads    uint

	pretty bool

	sizeLimit uint

	limit uint

	ifSortedKey bool

	delKeyExpireMax uint

	ifDirectDel bool

	version bool
}

func (this *CmdConf) parseCmdOptions() {
	flag.Usage = func() {
		this.printHelpMsg()
	}

	flag.StringVar(&this.host, "H", "127.0.0.1", "server host, default 127.0.0.1")
	flag.UintVar(&this.port, "P", 6379, "server  port, default 6379")
	flag.BoolVar(&this.forceAddr, "F", false, "if no suitable slave to scan|dump keys nor master to delete keys, use the redis as specified by -H and -P to scan|dump|delete keys")

	//flag.StringVar(&this.socket, "S", "", "server socket, overrides host and port")
	flag.StringVar(&this.passwd, "p", "", "password to use when connecting to the server")
	flag.UintVar(&this.database, "n", 0, "database number, default 0")

	//flag.BoolVar(&this.isCluster, "c", false, "if the server is redis cluster. default false")
	//flag.BoolVar(&this.ifAllNodes, "a", false, "for redis cluster, if iterate all nodes. default false")

	flag.StringVar(&this.commands, "w", "bigkey", "work type, default bigkey. valid option is one of "+strings.Join(g_validCmds, ","))
	flag.StringVar(&this.pattern, "k", "", "regular expression, only process keys match this. default processing all keys")
	flag.StringVar(&this.keyfile, "f", "", "read keys from this file and process them, one key one line.")
	flag.BoolVar(&this.dryRun, "dr", false, fmt.Sprintf("dry run, default false. Do not actually dump, delete or find big keys, Only write target keys to file %s. -si doesnot work in this work", gc_dryrun_keys_file))

	flag.StringVar(&this.outdir, "o", "", "output result to this dir. default current working dir")

	flag.UintVar(&this.keyBatch, "kc", 100, "sleep after scanning|processing this count of keys. default 100")
	flag.UintVar(&this.keyInterval, "ki", 5, "sleep for the time(microseconds), after scanning|processing -kc count of keys. default 5, no sleep if 0")

	flag.UintVar(&this.elementBatch, "ec", 500, "sleep after scanning|processing this count of elements of one key. default 500")
	flag.UintVar(&this.elementInterval, "ei", 2, "sleep for the time(microseconds), after scanning|processing -ec count of elements of one key. default 2, no sleep if 0")

	flag.UintVar(&this.processThreads, "t", 1, "threads concurrently to process keys, default 1")
	flag.UintVar(&this.scanThreads, "s", 1, "threads concurrently to scan keys, default 1")

	flag.BoolVar(&this.pretty, "b", false, "output pretty json format. default false, that is, the compact json format")

	flag.UintVar(&this.sizeLimit, "si", 1024, "for -w=dump|bigkey, only process keys whose size >= -si(bytes)")

	flag.UintVar(&this.limit, "li", g_default_uints["limit"], "for -w=bigkey and work with -st , only output the biggest -li keys, default 100")
	flag.BoolVar(&this.ifSortedKey, "st", false, "for -w=bigkey, sort result by size of key, default false. Attention, if too many keys, trident will use much memory")

	flag.UintVar(&this.delKeyExpireMax, "ex", gc_delete_expire_seconds_max, "for -w=delete, delete key by setting expiration to randon [1,-ex] seconds. default 60")

	flag.BoolVar(&this.ifDirectDel, "dd", false, "for -w=delete, directly delete keys by del [key]. default false")

	flag.BoolVar(&this.version, "v", false, "print version and exits")

	flag.Parse()

	if this.version {
		fmt.Printf("\n%s\n\t%s\n\n", gc_version, gc_usage_comment)
		os.Exit(0)
	}

	var err error
	/*
		if this.socket != "" {
			if !kitsFile.IsFile(this.socket) {
				ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf(""),
					logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_NOT_EXISTS, ehand.NAME_MSG: fmt.Sprintf("-S %s is not a file nor exists", this.socket)}, true)
			}
		}
	*/

	if this.commands != "bigkey" {
		if !kitsSlice.ContainsString(g_validCmds, this.commands) {
			ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf(""),
				logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_INVALID_OPTION, ehand.NAME_MSG: fmt.Sprintf("-w %s is invalid, valid option is one of %s",
					this.commands, strings.Join(g_validCmds, ","))}, true)
		}
	}

	this.patternReg, err = regexp.Compile(this.pattern)
	if err != nil {
		ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err),
			logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_REG_COMPILE, ehand.NAME_MSG: fmt.Sprintf("invalid regular expression: -k %s", this.pattern)}, true)
	}
	fmt.Println(this.patternReg.String())

	if this.keyfile != "" {
		if !kitsFile.IsFile(this.keyfile) {
			ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf(""),
				logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_NOT_EXISTS, ehand.NAME_MSG: fmt.Sprintf("-f %s is not a file nor exists", this.keyfile)}, true)
		}
	}

	if this.outdir != "" {
		if !kitsFile.IsExist(this.outdir) {
			ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf(""),
				logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_NOT_EXISTS, ehand.NAME_MSG: fmt.Sprintf("-o %s not exists", this.outdir)}, true)
		}
	} else {
		this.outdir, err = os.Getwd()
		if err != nil {
			ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf("%s", err),
				logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_DIR_GETCWD, ehand.NAME_MSG: "-o is not set and fail to get current working dir"}, true)
		}
	}

	if this.limit != g_default_uints["limit"] && !this.ifSortedKey {
		ehand.CheckErrNoExtraMsg(g_logger, errors.Errorf(""),
			logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OPTION_MISMATCH, ehand.NAME_MSG: "-li must work with -st"}, true)
	}
	/*
		if this.scanCnt > this.batch {
			logging.WriteToLogNoExtraMsg(g_logger, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK, ehand.NAME_MSG: fmt.Sprintf(
				"-s %d should not be greater than -b %d, set -b to %d", this.scanCnt, this.batch, this.scanCnt)}, logging.WARNING)
		}
	*/

}

func (this *CmdConf) printHelpMsg() {
	fmt.Printf("\nUsage ./trident options\n\t%s\n\t%s\n\n", gc_version, gc_usage_comment)
	flag.PrintDefaults()
}

func MaxInt(i int, j int) int {
	if i < j {
		return j
	} else {
		return i
	}
}

func MinInt(i int, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}
