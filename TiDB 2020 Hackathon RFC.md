# Proposal: tidbflashback

- Author(s): 刘博 邰晓 余建锋 孙天浩
- Last updated: 2021/01/07

## 概要
通过TiDB-Binlog实现TiDB版本的数据快速闪回，降低业务影响。

## 背景
MySQL的闪回一般是利用binlog完成的，能快速完成恢复且无需停机维护。从最早提交patch的阿里云彭立勋在MySQL5.5版本上就已实现，到后面基于伪slave拉取binlog的binlog2sql，MyFLash等等。尽管TiDB本身可以通过GC来快速恢复指定时间的数据快照，但这个取决于tikv_gc_life_time的时间，因此当需要恢复的时间已经超出这个GC生命周期那就没有办法了。因此我们有必要同样通过TiDB binlog来实现数据闪回，以备已经没有需要时间快照数据的场景。

## 方案
添加一个flashback参数，通过reparo解析指定时间段的TiDB binlog，生成经过反转的语句，可输出至文件。由于回滚是需要逆向恢复的，因此最终需要对生成的文件内容通过tac反转后进行恢复。

## 原理
通过官方TiDB Binlog 的一个配套工具reparo，增量解析TiDB binlog，并对解析出的文本进行处理，生成对应的反转语句，例如，INSERT反转成DELETE。文件生成后只需对文本内容进行逆序逐条apply即可。
