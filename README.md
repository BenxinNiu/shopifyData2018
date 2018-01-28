# shopifyData2018

Written in scala with spark apis

sample usage

navigat to project foler in terminal 

run 

sbt "run patht_o_file_1 patht_o_file_2 join_type condition"

e.g

sbt "run /home/benxin/shop/customers.json /home/benxin/shop/orders.json inner cid=customer_id"

OR use spark submit to run the jar
