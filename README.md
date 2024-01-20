# Quantitative Investment

This repository contains the resources and instructions for setting up and running the Quantitative Investment project.

## Table of Contents

- [Quantitative Investment](#quantitative-investment)
  - [Table of Contents](#table-of-contents)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Database and Results](#database-and-results)
  - [Contact](#contact)

## Prerequisites

- Python 3
- Django-q

## Installation

**Set up a virtual environment**

Follow the instructions on [this link](https://ithelp.ithome.com.tw/articles/10199980) to create a virtual environment.

## Usage

**Start the Django-q automatic scheduler**

Instructions to set up Django-q can be found [here](https://peilee-98185.medium.com/python-django-%E7%9A%84%E6%93%B4%E5%85%85%E6%87%89%E7%94%A8-django-q-%E6%8E%92%E7%A8%8B%E5%B7%A5%E5%85%B7-8f9e66182814).

Start the server by running:

```python
python manage.py runserver
```

Start the Django-q cluster by running:

```python
python manage.py qcluster
```

You can now schedule tasks by logging into the admin panel at:

```python
localhost:8000/admin
```

Username: `tonysu`
Password: `12345678`

**Tips**

1. Use the cash flow statements in the database. The data is for a single quarter, so you need to use `to_seasonal` to convert it to a single quarter format.
2. When parsing quarterly reports, just throw the financial statements in. Otherwise, indexing will take a long time.

## Database and Results

The database and result files can be found [here](https://uillinoisedu-my.sharepoint.com/personal/boyusu2_illinois_edu/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fboyusu2%5Fillinois%5Fedu%2FDocuments%2FQuantX&ga=1).

## Contact

If you encounter any issues or have any questions, please open an issue on this repository.
