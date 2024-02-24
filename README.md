# **Video streaming analyzer by ip url**
## **Startup requirements**
- You must have **_python v3.11_** and **_pip3_** installed on your pc.
> It is also necessary to execute the command.
<br> Without the logger installed, the program will not be able to build.
```
pip install loguru==0.7.2
```
> or
```
pip3 install loguru==0.7.2
```

- The pc must have **_docker_** and **_docker compose v3_** installed.

## Instructions for use
- First you need to run the **build.py** script in the root of the proxy to build the docker cluster where the application is hosted.
- When the build is finished, you need to go inside the **myapp** container via the docker desktop application. 
<br> (Inside the docker, it is better to work from root user)
> If there is no docker application, you can use the command 
```
docker exec -ti {{ container name }} /bin/sh
``` 
> or 
```
docker exec -it {{ container name }} sh command
```
- At the path **/app/src** there will be a script **start.sh** which should be run from the directory where it is located.
> Use
```
cd /app/src

./start.sh
```
- Note: the application starts with a delay if all the above operations were done before the services required for operation were initialized.

## Possible problems
- When restarting the cluster, make sure that all containers are started correctly. There is a possibility that they will be raised 
in the wrong order and will not be started when restarting the cluster
> Dependence of containers on each other ((zookiper >> kafka >> kafka_ui), redis) >> myapp >> nginx
- Containers have an internal virtual network, but myapp proxies to local port 9999 and if it gets busy the app may not be available. 
Also nginx access via port 80 is provided.
- If you change the ip in the **_.env_** for the docker build, the project won't work. Autopulling ip from **_.env_** to code will be added in release 1.1.
