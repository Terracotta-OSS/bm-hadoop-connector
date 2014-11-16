BigMemory Hadoop Connector
===========================
## Introduction

The BigMemory Connector for Hadoop helps output of Apache Hadoop jobs can be loaded directly into BigMemory Max, greatly reducing
the time it takes to gain access to job results. The BigMemory Connector is designed for Hadoop Developers
and Application Architects who have existing BigMemory Max or distributed Ehcache installations backed by
a Terracotta Server Array.


## Project structure:
The repository contains 2 projects as listed below.

1. bigmemory-hadoop :

		This project implements custom Hadoop Map-Reduce classes required which are required to publish output of map-reduce program into Big Memory Max.
		
2. bigmemory-wordcount:

	This project demonstrates an word count example  using bigmemory-hadoop connector where result of map-reduce job is published into BigMemory Max at runtime.


## How to build:
Simply clone this repo and run mvn clean install






