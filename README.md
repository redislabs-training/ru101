# Redis University RU101: Introduction to Redis Data Structures

## Introduction

This repo contains the sample data and Python code for [RU101, Introduction to Redis Data Structures](https://university.redis.com/courses/ru101/) at [Redis University](https://university.redis.com/).

## Setup

You'll need to clone this repository to your machine, and setup Redis and Python as described below.

### Redis

There are multiple ways that you can install Redis, options include:

* [Download the source code](https://redis.io/download) and build Redis from source.
* Use the [Redis container from Docker Hub](https://hub.docker.com/_/redis/).
* Mac OS users can install Redis using the [Homebrew Package Manager](https://brew.sh/).
* Windows 10 users can install Redis using WSL 2.  [This video shows you how](https://www.youtube.com/watch?v=_nFwPTHOMIY), and this [blog post](https://redis.com/blog/redis-on-windows-10/) provides additional guidance.
* Debian and Ubuntu users can install Redis using the apt package manager - look for the `redis-server` package.

You should install the latest Redis 6 release if possible.  If you're unable to use Redis 6, make sure to install Redis 5.0.3 or higher.

### Python

This course requires Python 3.  You should create and use a virtual environment when running the code and loading the sample data into Redis.  You'll also need to set your `PYTHONPATH` environment variable:

```bash
$ git clone https://github.com/redislabs-training/ru101.git
$ cd ru101
$ export PYTHONPATH=`pwd`
$ python3 -m venv venv
$ . venv/bin/activate
$ pip install -r requirements.txt
```

### Configuration

The data loader and sample code expect Redis to be running at `localhost:6379` with no password set.  This is the default, but if your Redis instance uses a different hostname, port, or has a password, you should set the appropriate environment variables:

```bash
$ export REDIS_HOST=myredishost
$ export REDIS_PORT=9999
$ export REDIS_PASSWORD=ssssh
```

### Network Latency

Running the Redis server and the Python code on different machines introduces round trip network latency for each Redis command sent from Python to Redis. To keep the example code simple, some of the Python scripts for this course send each command to Redis separately. 

To get the most performance from applications using Redis, we recommend the use of [pipelining](https://redis.io/topics/pipelining) to send multiple commands to the server in a single round trip where possible. You should also ensure that the application code and Redis server are on hosts with minimal latency between them. These concepts are covered in more detail in the following programming language specific Redis University courses:

* [RU102J Redis for Java Developers](https://university.redis.com/courses/ru102j/)
* [RU102JS Redis for JavaScript Developers](https://university.redis.com/courses/ru102js/)
* [RU102PY Redis for Python Developers](https://university.redis.com/courses/ru102py/)

To get the best performance from the simple demo scripts for this course, you should run the code and the Redis server on the same local network or the same machine if possible.

### Loading the Sample Data

To answer some of the homework and exam questions, and to run the sample code, you'll need to load the sample data into Redis:

```bash
$ cd redisu
$ python utils/dumpload.py load ru101/data/ru101.json
```

If the sample data was loaded correctly, you should see the following message:

```
total keys loaded: 14328
```

### Using redis-cli

Some of the quiz, homework and exam questions for this course may require you to use the Redis command line interface to execute commands.  Start redis-cli from the command line as follows:

```bash
$ redis-cli
127.0.0.1:6379>
```

You can then enter Redis commands, for example to see how many keys are in the database use `dbsize`:

```bash
127.0.0.1:6379> dbsize
(integer) 14328
127.0.0.1:6379>
```

If you prefer a more graphical user interface, try [RedisInsight](https://redis.com/redis-enterprise/redis-insight/), a free tool for exploring and interacting with Redis that includes an embedded redis-cli.

### Running the Code

To run the example code, `cd` to the directory containing the script you wish to run, and pass the script name to the `python` command.

Example:

```bash
$ cd redisu/ru101/uc01-faceted-search
$ python search.py
```

### Need Help or Have Questions?

If you need help, have questions, or want to chat about all things Redis, please join us on the [Redis University Discord Server](https://discord.gg/PxxqQg5).

### Subscribe to our YouTube Channel

We'd love for you to [check out our YouTube channel](https://youtube.com/redisinc), and subscribe if you want to see more Redis videos!
