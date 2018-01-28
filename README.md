# shopifyData2018

Written in scala with spark apis

sample usage

navigate to project foler in terminal 

run 

sbt "run path_to_file_1 path_to_file_2 join_type condition"

e.g

sbt "run /home/benxin/shop/customers.json /home/benxin/shop/orders.json inner cid=customer_id"

OR use spark submit to run the jar
