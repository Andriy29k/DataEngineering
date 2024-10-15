FROM python:3.12-slim

RUN pip install jupyterlab

RUN pip install numpy pandas matplotlib seaborn scipy

RUN pip install sqlalchemy psycopg2-binary

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--LabApp.token=''"]