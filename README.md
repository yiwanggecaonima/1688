# A1688
代码分离

先获取全部url 存入redis
使用selenium渲染获取url item包括key 就是搜索的关键字作为tag url链接  组成的dict
然后hset进去 使用的是hashlib这个库

redis代码写的不是很好  
最好把redis的操作封装成类方法 方便调用


提取url的信息
使用异步请求
这里的aiphttp比较灵活
可以用for循环来运行异步代码  加入task
但是既然用的是redis 那么应该可以使用while 循环  pop()的方法

爬取的数据存储 用异步数据库来存储  既然异步嘛 那就异步到底   这里用到陈大佬的logger文件 和 异步存储数据库的文件  现成的 通用的 


