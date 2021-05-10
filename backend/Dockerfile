FROM amazoncorretto:11.0.9-alpine
COPY build/libs/*.jar de-be.jar
RUN mkdir -p /home/spring/uploaded-cv
RUN chmod 755 /home/spring/uploaded-cv
ENTRYPOINT ["java","-jar","/de-be.jar"]