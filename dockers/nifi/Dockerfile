FROM apache/nifi:1.25.0
USER root
RUN apt update && apt install -y vim
USER nifi
ENTRYPOINT ["../scripts/start.sh"]