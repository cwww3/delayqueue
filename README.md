## 延迟队列
基于redis的分布式延迟队列

- 通过 zset和list实现
- 通过watch+pipeline实现从zset迁移至list(如果插入频繁zset频繁更新，会导致迁移成功率降低)

### TODO
- lua脚本 提升迁移成功率
- 分布式锁 避免不同实例同时执行迁移代码
- 根据zset中的第一个元素来确定等待时间代替轮询(需要监听新加入zset的元素)