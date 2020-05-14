FROM python:3.6-alpine

# RUN pip install pydevd

COPY tracker.py /code/
COPY start.sh /code/
WORKDIR /code

EXPOSE 4321

CMD [ "sh", "start.sh"]
