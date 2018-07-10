# General Configuration
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


