服务器端命令：
nc -l ip地址 端口号
D:\soft\netcat\nc -l -p 9999
输入接受到的数据

客户端命令：
nc ip地址 端口号 < 发送的文件名
D:\soft\netcat\nc 127.0.0.1 9999 < D:\soft\netcat\readme.txt

先启动服务端命令，监听机器端口号
在启动客户端命令，向服务端机器对应端口发送数据