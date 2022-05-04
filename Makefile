kafka-terminal:
	docker-compose build
	docker-compose up

build:
	docker-compose build
	docker-compose up -d

rebuild:
	docker-compose up --build

ingress:
	javac GraphAnalyticsFilesApp.java
	java GraphAnalyticsFilesApp
	make clean

latency-test:
	javac LatencyTest.java
	java LatencyTest
	make clean

clean:
	rm -rf *.class
