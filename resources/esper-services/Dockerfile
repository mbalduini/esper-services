FROM openjdk

# Installation dir
ENV ES_HOME /etc/esper-services

# Install all needed files
COPY bin ${ES_HOME}/bin
COPY data ${ES_HOME}/data
COPY lib ${ES_HOME}/lib
COPY esper-services-0.9.jar ${ES_HOME}/esper-services-0.9.jar
COPY test-producer.jar ${ES_HOME}/test-producer.jar

RUN chmod u+x ${ES_HOME}/bin/start-esper-rest-server.sh
RUN chmod u+x ${ES_HOME}/bin/test-producer.sh
RUN mkdir ${ES_HOME}/config
RUN mkdir ${ES_HOME}/additional-components

WORKDIR ${ES_HOME}

#EXPOSE 3456
#EXPOSE 3457

ENTRYPOINT  ["./bin/start-esper-rest-server.sh"]
CMD ["./config/config.json"]