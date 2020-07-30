#!/bin/sh

docker-compose exec mids ab -n 150 -H "Host:user1.comcast.com" http://localhost:5000/purchase_a_sword
docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/join_guild
docker-compose exec mids ab -n 100 -H "Host: user2.comcast.com" http://localhost:5000/purchase_a_sword
docker-compose exec mids ab -n 15 -H "Host: user2.comcast.com" http://localhost:5000/join_guild
docker-compose exec mids ab -n 210 -H "Host: user3.comcast.com" http://localhost:5000/purchase_a_sword
docker-compose exec mids ab -n 7 -H "Host: user3.comcast.com" http://localhost:5000/join_guild
