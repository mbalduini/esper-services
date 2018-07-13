
# Documentation
The documentation of the API can be found online at: https://documenter.getpostman.com/view/4752228/RWM8Rqsu

The documentation of the Esper and EPL can be found online at: http://esper.espertech.com/release-7.1.0/esper-reference/html/index.html

# Preliminary Step

## Docker

https://docs.docker.com/install/linux/docker-ce/centos/

**Install Docker**

  sudo yum install -y yum-utils \
    device-mapper-persistent-data \
    lvm2
                 
  sudo yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo

  sudo yum install docker-ce

**Start Docker**

  sudo systemctl start docker

**Test Docker**

  sudo docker run hello-world

## Build the Docker container:

Run only if needed (missing internet connection)

  docker build -t <image-name> .

e.g.

  docker build -t brembo/esper-services:1 .

## Run the Docker container:

  docker run -it \
  -v <local-config-folder-path>:/etc/esper-services/config \
  --name <docker-name> (optional)\
  -p <container-http-port>:<local-http-port> \
  -p <container-ws-port>:<local-ws-port> \
  <image-name>

e.g. 

  docker run -it \
  -v /Users/baldo/Documents/Work/git/esper-services/resources/esper-services/config:/etc/esper-services/config \
  -p 3456:3456 \
  -p 3457:3457 \
  mbalduini/esper-services

## List Containers
**Active Containers**

  docker ps

**All Containers:** 

  docker ps -a

## Remove Container

  docker rm <image-name>

## Create and fill config folder on the VM
The local config folder will be shared with the Docker container and will contain the configuration file of the server

  mkdir config
  cd config
  vi config.json

Copy the default and save the config file in the folder

# Configuration
The server can be configured via a simple json file.

    {
      "port" : (optional - int - default: 3456) port of the rest server ,
      "wsServerPort" : (optional - int - default: 3457) port of the websocket server,
      "logConfigurator" : (optional - String - default: a default configuration is inside the classPath) path of the configuration file for log4j
    }

By default, the server uses the json file in config folder (config.json)

# Hints For Demonstrative Running

## SQLServer Scripts for creating and fill tables

### Result table for JDBC consumer

    CREATE TABLE dbo.result (
      id bigint(8) NOT NULL IDENTITY(1,1),
      varId int NULL,
      value real(4) NULL,
      ITEMID varchar(45) NULL,
      devSn varchar(45) NULL,
      onTime varchar(45) NULL,
      STARTDATETIME varchar(45) NULL,
      ENDDATETIME varchar(45) NULL
    );
    
    ALTER TABLE dbo.result ADD CONSTRAINT PK__result__3213E83F593A512F PRIMARY KEY (id);

### Performance table for throughput test

    CREATE TABLE dbo.performance (
      id bigint(8) NOT NULL IDENTITY(1,1),
      varId int NULL,
      value real(4) NULL,
      devSn varchar(45) NULL,
      onTime varchar(45) NULL
    );
    
    ALTER TABLE dbo.performance ADD CONSTRAINT PK__performa__3213E83F5F2107C6 PRIMARY KEY (id);

### StaticData table for join

    CREATE TABLE dbo.staticData (
      bb_s_n varchar(20) NOT NULL,
      MACHINE varchar(20) NOT NULL,
      CDI varchar(20) NOT NULL,
      ITEMID varchar(20) NOT NULL,
      STARTDATETIME varchar(50) NOT NULL,
      ENDDATETIME varchar(50) NOT NULL
    );
    
    ALTER TABLE dbo.staticData ADD CONSTRAINT PK__staticDa__6ABD24690D3EFD26 PRIMARY KEY (bb_s_n, MACHINE, CDI, ITEMID, STARTDATETIME, ENDDATETIME);

### Fill Static data table

  INSERT INTO dbo.staticData (bb_s_n, MACHINE, CDI, ITEMID, STARTDATETIME, ENDDATETIME) VALUES
  ('IOTSPI216110315', 'A561322', '182CBAFBRY53', '''20A09132_WIP''', '2018-03-26 00:32:56', '2018-03-26 02:31:13'),
  ('IOTSPI216110315', 'A561322', '182CBAFBRY76', '''20A09131_WIP''', '2018-03-26 00:41:53', '2018-03-26 02:31:13'),
  ('IOTSPI216110315', 'A561322', '182CBAFBRZ46', '''20A09131_WIP''', '2018-03-26 02:32:15', '2018-03-26 05:35:27'),
  ('IOTSPI216110315', 'A561322', '182CBAFBRZ48', '''20A09132_WIP''', '2018-03-26 02:32:49', '2018-03-26 05:35:27'),
  ('IOTSPI216110315', 'A561322', '182CBAFBRZ46', '''20A09131_WIP''', '2018-03-26 06:14:16', '2018-03-26 06:34:00'),
  ('IOTSPI216110315', 'A561322', '182CBAFBRZ48', '''20A09132_WIP''', '2018-03-26 06:14:27', '2018-03-26 06:34:00'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSB43', '''20A09131_WIP''', '2018-03-26 06:49:56', '2018-03-26 10:37:26'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSB44', '''20A09132_WIP''', '2018-03-26 06:50:12', '2018-03-26 10:37:26'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSF47', '''20A09131_WIP''', '2018-03-26 10:37:50', '2018-03-26 11:47:15'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSF48', '''20A09132_WIP''', '2018-03-26 10:38:05', '2018-03-26 11:47:15'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSF48', '''20A09132_WIP''', '2018-03-26 12:05:28', '2018-03-26 14:09:35'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSF47', '''20A09131_WIP''', '2018-03-26 12:06:01', '2018-03-26 14:09:35'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSJ32', '''20A09131_WIP''', '2018-03-26 15:41:31', '2018-03-26 15:42:37'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSJ33', '''20A09132_WIP''', '2018-03-26 15:41:47', '2018-03-26 15:42:37'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSJ33', '''20A09132_WIP''', '2018-03-26 15:52:37', '2018-03-26 18:02:20'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSL35', '''20B28832_WIP''', '2018-03-26 18:02:30', '2018-03-26 18:08:47'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSL35', '''20B28832_WIP''', '2018-03-26 18:09:48', '2018-03-26 21:50:15'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSL44', '''20B28831_WIP''', '2018-03-26 18:13:22', '2018-03-26 21:50:15'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSL44', '''20B28831_WIP''', '2018-03-26 22:02:36', '2018-03-26 23:49:33'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSL35', '''20B28832_WIP''', '2018-03-26 22:02:58', '2018-03-26 23:49:33'),
  ('IOTSPI216110315', 'A561322', '182CBAFBSL35', '''20B28832_WIP''', '2018-03-26 23:28:06', '2018-03-26 23:28:07');