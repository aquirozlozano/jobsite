# jobsite
We are going to use docker-compose to deploy postgres db and airflow instance
where we will execute our dag
## STEPS
deploy ec2 instance Amazon Linux 2 AMI (HVM) - Kernel 4.14, SSD Volume Type
deploy redshift db
update SO 
```bash
sudo yum update -y
```

#start docker
```bash
sudo service docker start
systemctl status docker
```

#fix errors 
```bash
sudo usermod -a -G docker ec2-user
sudo chmod 666 /var/run/docker.sock
```

#install docker-compose
```bash
pip3 install docker-compose
```

#deploy containers
```bash
docker-compose up
```