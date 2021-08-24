FROM jupyter/scipy-notebook

ENV JUPYTER_ENABLE_LAB=yes
COPY . /var/code
WORKDIR /var/code

