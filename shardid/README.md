# shardid

## 64 BITS
| signed 1 |  millis (39)  |  worker(2) | db-sharding(10)| table-rotate(2)  | sequence(10)  |
|----------|---------------|------------|----------------|------------------|---------------|
            39 = 17 years        2 = 4      10=1024         0: none    :table         10=1024
                                                            1: monthly :table-[YYYYMM]
                                                            2: weekly  :table-[YYYY0XX]
                                                            3: daily   :table-[YYYYMMDD]
- signed(1):        sid is always positive number
- millis(39):       2^39 (17years) unix milliseconds since 2024-02-19 00:00:00
- workers(2):       2^2(4) workers
- db-sharding(10):  2^10 (1024) database instances
- table-rotate(2):  2^2(4) table rotate: none/by year/by month/by day
- sequence(10):     2^10(1024) ids per milliseconds
  
## TPS:
- ID: 1000(ms)*1024(seq)*4 = 4096000  409.6W/s
      1000*1024            = 1024000  102.4W/s

- DB : 
      10   * 1000   =   10000       1W/s
      1024 * 1000   = 1024000   102.4W/s

      10   * 2000   =   20000       2W/s
      1024 * 2000   = 2048000   204.8W/s

      10   * 3000   =   30000       3W/s
      1024 * 3000   = 3072000   307.2W/s

## mysql-benchmark 
- https://docs.aws.amazon.com/whitepapers/latest/optimizing-mysql-on-ec2-using-amazon-ebs/mysql-benchmark-observations-and-considerations.html
- https://github.com/MinervaDB/MinervaDB-Sysbench
- https://www.percona.com/blog/assessing-mysql-performance-amongst-aws-options-part-one/

## issues
- Overflow capacity
  waiting for next microsecond.

- System Clock Dependency
  You should use NTP to keep your system clock accurate.

- Time move backwards   
  + if sequence doesn't overflow, let's use last timestamp and next sequence. system clock might moves forward and greater than last timestamp on next id generation 
  + if sequence overflows, and has to be reset. let's built-in clock to get timestamp till system clock moves forward and greater than built-in clock

- Built-in clock
  record last timestamp in memory/database, increase it when it is requested to send current timestamp instead of system clock