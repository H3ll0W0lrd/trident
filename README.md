# 简介
    trident可以安全地分析出redis与redis cluster所有的big key，可以安全地删除匹配某个正则的所有KEY， 可以安全地dump出匹配某个正则的所有KEY的内容<br />
    trident解决了redis-cli --bigkeys抽样与list等复合类型的KEY不准确的问题， 也解决了现在某些同类型的工具速度慢的问题<br />
    在唯品会内部比较多应用的是扫描出所有redis/redis cluster中的big key, 扫描出匹配某个正则的KEY，dump特定的KEY的内容来进行debug<br />\
    有任何的bug或者使用建议欢迎反馈laijunshou@gmail.com。
        1）生成csv格式的bigkey报表， 指定--st --li xxx会按KEY大小排序并输出最大的XXX个KEY：
            ./trident -H xx -P xx  -s 1 -t 3 -w bigkey -ec 100 -ei 2 -kc 100 -ki 10  -si 10 -li 3 -st -n 0
![bigkey](https://github.com/GoDannyLai/trident/raw/master/misc/img/bigkeys.png)

        2）dump出指定的KEY的value
            ./trident -H xx -P xx  -s 8 -t 16  -b -si 0 -w dump -k "discovery_\d+_key" -ec 100 -ei 2 -kc 100 -ki 10  -b
            redis_dump.json包含了目标KEY的VALUE
            $ cat redis_dump.json
                #one key#
                {
                        "Database": 0,
                        "Bytes": 89,
                        "ElementCnt": 1,
                        "Type": "hash",
                        "Expire": "",
                        "MaxElementBytes": 54,
                        "Key": "discovery_14189366_key",
                        "MaxElement": "great_comment",
                        "Value": {
                                "great_comment": "{\"6575\":{\"id\":6575,\"timeStamp\":1485384264191}}"
                        }
                }
        3) 删除指定的KEY
            ./trident.exe -H xx -P xx  -s 1 -t 1 -w delete -ec 100 -ei 2 -kc 100 -ki 10 -n 0 -ex 10 -k "name\d+"
            deleted_keys.txt文件包含了所删除的KEY名
            $ cat deleted_keys.txt
                name48
                name37
                name43
                name73


# 功能与特点
    1）支持redis与redis cluster, 无指定， 自动识别, 只需指定一个实例的IP与端口，如果是redis cluster则会扫描整个集群。
        对于cluster， 会并行地扫描与处理每个实例
    2）速度与安全
        需要注意的是， 由于网络回来多， 远程扫描时要快点可以增加线程与减少SLEEP间隔， 注意看下QPS与redis的CPU， 如果是直接在本地扫描，则会很快<br />
        CPU与QPS会很高。 由于都是分批操作， 很多网络回来， 速度不快， 处理12万KEY， 需要3分钟， CPU很低， 才3%， 但放到本地扫描， 只要3秒钟， 但CPU 60%<br />
        注意自己线上的情况而调整线程与SLEEP有间隔。 另外trident不管你是指定了从库还是主库，都是到从库去扫描， 只有删除KEY才会到主库执行<br />
        
        2.1) 每个实例最多一个扫描线程， 但可以有多个处理线程
            --s x
            --t y
            启动x个线程去扫描redis的key名， y个线程去处理扫描线程获取的KEY名。可以根据自己的需要调整
            --kc aa
            --ki bb
            每扫描或处理aa个KEY时， 扫描或处理线程休眠bb微秒 
        2.2）扫描bigkey时， 对于list等复合类型的KEY， 处理线程会分批次scan出所有的元素， 不会一次性取出所有的元素从而阻塞redis
            --ec xx
            --ei yy
            对于list等复合类型的KEY， 处理线程每扫描XX个元素就维休眠yy微秒    
        2.3）不管你是指定了从库还是主库，都是到从库去扫描， 只有删除KEY才会到主库执行
           无论指定的是master还是slave, 都会自动识别，寻找最新的从库去扫描KEY， 到最新的从库去获取KEY的值， 到主库去删除KEY。 如果不是删除操作， 是不会查询主库的
        2.4）提供了dry run功能， 以免操作错数据
            --dr
                把目标KEY名写入redis_target_keys.txt，每行一个KEY。
                确定后，指定从redis_target_keys.txt读取要处理的KEY名。
            --f redis_target_keys.txt
        2.5)正则匹配目标的KEY
            --k "xx"
            只处理匹配到XX正则表达式的KEY
        2.6）删除KEY时并不会直接del， 而是设置一个范围内的随机时间过期，防止del大KEY造成阻塞
            --ex yy
                给目标KEY设置[1, yy]内的随机过期时间来删除KEY
            --dd
                直接del KEY
    3） 只输出大于某个SIZE的最大N个KEY， 并按大小排好序
        --st 
            按KEY的大小排序
        --si xx
            只输出SIZE大于xx bytes的KEY。当指定--st排序时， 最好设置--si， 一来是防止排序大量的KEY带来的大量内存，二来1KB以下小KEY一般也不care
        --li yy 
            只输出最大的yy个KEY