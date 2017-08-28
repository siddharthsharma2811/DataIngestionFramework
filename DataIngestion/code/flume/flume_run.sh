/bin/flume-ng agent -n agent1 -f /home/ec2-user/flume_demo/flume_file_channel.conf


/bin/flume-ng agent --conf-file /home/ec2-user/flume/flume_memory_channel.conf --name IB_Flume_Agent -Dflume.root.logger=INFO,console 