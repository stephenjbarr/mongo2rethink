FROM stephenjbarr/r-ubuntu-vivid-docker

MAINTAINER Stephen J. Barr <stephenjbarr>

RUN mkdir /mongo2rethink
ADD mongo2rethink-generic.py /app/mongo2rethink-generic.py
ADD sjbsettings.py /app/sjbsettings.py
ENTRYPOINT ["/app/mongo2rethink-generic.py"]

