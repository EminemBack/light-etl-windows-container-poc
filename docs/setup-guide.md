# Setup Guide - Light ETL Windows Container POC

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Running the Application](#running-the-application)
5. [Testing](#testing)
6. [Troubleshooting](#troubleshooting)
7. [Production Deployment](#production-deployment)

## Prerequisites

### System Requirements

#### For Windows Host (Required for File Server)
- Windows 10/11 Pro or Enterprise (Build 1809 or later)
- Windows Server 2016/2019/2022
- Docker Desktop for Windows with Windows containers support
- At least 16GB RAM (Windows containers are memory-intensive)
- 50GB free disk space

#### For Linux/Mac Development
- Docker and Docker Compose installed
- Python 3.11+ (for local testing)
- 8GB RAM minimum
- 20GB free disk space

### Software Requirements
- Git
- Docker Desktop (with Windows containers enabled for Windows hosts)
- Python 3.11+ (optional, for local development)
- SQL Server or SQL Server Express (for database)
- Network share mounted as Z:\ drive (or configurable path)

## Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/light-etl-windows-container-poc.git
cd light-etl-windows-container-poc