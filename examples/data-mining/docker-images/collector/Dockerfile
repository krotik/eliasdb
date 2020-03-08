FROM python:alpine
RUN apk update && apk add --no-cache supervisor

RUN pip install schedule requests

ADD etc/supervisord.conf /etc/supervisord.conf
ADD app/main.py /app/main.py

RUN adduser -D collector
RUN chown collector:collector /app -R
USER collector

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
