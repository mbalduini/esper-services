FROM openjdk

# Installation dir
ENV PT_HOME /etc/performance-test

# Install all needed files
COPY bin ${PT_HOME}/bin
COPY data ${PT_HOME}/data
COPY lib ${PT_HOME}/lib
COPY esper-services-0.9.jar ${PT_HOME}/esper-services-0.9.jar

RUN chmod u+x ${PT_HOME}/bin/start-JDBC-test.sh
RUN chmod u+x ${PT_HOME}/bin/start-MQTT-dataTransf-test.sh
RUN chmod u+x ${PT_HOME}/bin/start-MQTT-test-custom-class.sh
RUN chmod u+x ${PT_HOME}/bin/start-MQTT-test.sh
RUN chmod u+x ${PT_HOME}/bin/start.sh
RUN mkdir ${PT_HOME}/config

WORKDIR ${PT_HOME}

ENTRYPOINT  ["./bin/start.sh"]
CMD ["/etc/performance-test"]
