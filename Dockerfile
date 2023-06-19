FROM java:8
COPY build/libs/tracking-1.0.0.jar /app/run.jar
ENTRYPOINT exec java $JAVA_OPTIONS $JAVA_MAX_HEAP_SIZE $JAVA_START_HEAP_SIZE -jar /app/run.jar

