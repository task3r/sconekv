FROM python:3.7-alpine

# RUN pip install pydevd

COPY tracker.py /code/
COPY start.sh /code/
WORKDIR /code

EXPOSE 4321

CMD [ "sh", "start.sh"]
