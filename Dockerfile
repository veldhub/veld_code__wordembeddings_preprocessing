FROM debian:bookworm-20240812
RUN apt update
RUN apt install -y python3
RUN apt install -y python3-pip
RUN pip3 install --break-system-packages ipdb

