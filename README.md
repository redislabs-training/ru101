# Redis University RU101: Introduction to Redis Data Structures

## Introduction

This repo contains the sample data and Python code for [RU101, Introduction to Redis Data Structures](https://university.redislabs.com/courses/ru101/) at [Redis University](https://university.redislabs.com/).

## Setup

You'll need to clone this repository to your machine, and setup Redis and Python as described below.

### Redis

There are multiple ways that you can install Redis, options include:

* [Download the source code](https://redis.io/download) and build Redis from source.
* Use the [Redis container from Docker Hub](https://hub.docker.com/_/redis/).
* Mac OS users can install Redis using the [Homebrew Package Manager](https://brew.sh/).
* Windows 10 users can install Redis using WSL 2.  This [blog post](https://redislabs.com/blog/redis-on-windows-10/) provides guidance for this.
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

### Loading Sample Data

To answer some of the homework and exam questions, and run the sample code, you'll need to load the sample data into Redis:

```bash
$ python utils/dumpload.py load ru101/data/ru101.json
```

If the sample data was loaded correctly, you should see the following message:

```
total keys loaded: 14328
```

### Running the Code

To run the example code, `cd` to the directory containing the script you wish to run, and pass the script name to the `python` command.

Example:

```bash
$ cd redusu/ru101/uc01-faceted-search
$ python search.py
```

### Need Help or Have Questions?

If you need help, have questions, or want to chat about all things Redis, please join us on the [Redis Universiry Discord Server](https://discord.gg/PxxqQg5).