This is the git for the master/service network and slave nodes of the IFoT platform.  
Right now this works in conjunction with the service broker since only API accessible via POST are the contents of this.  
Service Broker: https://github.com/linusmotu/IFoT-Service_Broker  

To be added:  
Resource broker  
-----
Please use the distributed/wip branch for Pi and distributed/nuc-wip branch for nucs.  
However I still need to updated the nuc-wip branch to include the heatmap.  

Right now there are a few hard coded information here located in the ff:  
1. /frontend/project/server/config.py  
    the redis url needs to be specified if you will put it in a separate node, but if not, do not change this.
2. /frontend/project/server/api/views.py  
    - Look for code with the string "http://" to see the hard coded ones. The hard coded information is for the location of the influxDB.  
    - I think i can move this to the CONFIG.py in the future.  

Notes:  
- Need to copy the models to /backend/models  
- Need to copy the chunks to /frontend/project/instance/htmlfi/Chunks  
- Need to copy the backend directory to the same path in the slave nodes. (ie. ~/flask-ifot/backend)  
  
  
Setup the following on the master node:  
- Raspberry Pi, NUC  
  - Requires:  
      - Git  
      - Docker  
- Install docker first.  
  - curl -sSL https://get.docker.com | sh  
  - sudo systemctl enable docker  
  - sudo systemctl start docker  
  - sudo usermod -aG docker pi  
  - pip install docker-compose (probably not the best method)  

Git clone required files, different branches for nuc and pi, nuc is not up to date. Will update soon.  
Git clone -b distributed/wip https://github.com/linusmotu/IFoT-middleware.git flask-ifot  
Git clone -b distributed/nuc-wip https://github.com/linusmotu/IFoT-middleware.git flask-ifot  
  
Edit /etc/docker/daemon.json  
This is when you need to communicate with the resource broker, but this will still work without it  
{ "insecure-registries":["RESOURCE_BROKER_IP:5000"] }  
  
Cd flask-ifot  
Docker-compose build  
docker swarm init --advertise-addr <MASTER-IP>  
This will return a docker swarm join command with a token  
- docker swarm join --token <token> <myvm ip>:<port>  

Just copy and run the command on the desired slave nodes  
- docker stack deploy --compose-file docker-compose.yml flask-ifot  

Before running this command, make sure the required files are in the slave nodes.  
This will start all the services listed in docker-compose  
